import boto3
import json
from datetime import datetime
import mysql.connector
import os
import base64
import logging
import traceback
import re
import random
import string
import zanolambdashelper

database_details = zanolambdashelper.helpers.get_db_details()

rds_host = database_details['rds_host']
rds_port = database_details['rds_port']
rds_db = database_details['rds_db']
rds_user = database_details['rds_user']
rds_region = database_details['rds_region']

database_dict = zanolambdashelper.helpers.get_database_dict()

rds_client =  zanolambdashelper.helpers.create_client('rds') 

zanolambdashelper.helpers.set_logging('INFO')



def delete_pool(cursor,pool_id):
    try:
        logging.info("Deleting pool...")
        sql = f"""  
            WITH RECURSIVE PoolHierarchy AS (
              SELECT poolid, parentid
              FROM {database_dict['schema']}.{database_dict['pools_table']}
              WHERE poolid = %s
              UNION ALL
              SELECT p.poolid, p.parentid
              FROM {database_dict['schema']}.{database_dict['pools_table']} p
              INNER JOIN PoolHierarchy ph ON p.parentid = ph.poolid
            )
            DELETE FROM {database_dict['schema']}.{database_dict['pools_table']} 
            WHERE poolid IN (SELECT poolid FROM PoolHierarchy) AND parentid is not null;
            """
        cursor.execute(sql, (pool_id,))
    except Exception as e:
        logging.error(f"Error fetching user identities: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        pool_id_raw = body_json.get('pool_id')

        variables = {
            'pool_id': {'value': pool_id_raw['value'], 'value_type': pool_id_raw['value_type']},
        }
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        pool_id = variables['pool_id']['value']

        with conn.cursor() as cursor:

            login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            zanolambdashelper.helpers.is_target_pool_in_org(cursor,database_dict['schema'],database_dict['pools_table'], organisation_id, pool_id)

            delete_pool(cursor, pool_id)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation 
            body_value = e.args[1]
        else:
            body_value = 'Unable to delete pool'
        error_response = {
            'statusCode': status_value,
            'body': body_value,
        }
        return error_response
       
    finally:
        try:
            cursor.close()
            conn.close()
        except NameError:  # catch potential error before cursor or conn is defined
            pass

    return {
        'statusCode': 200,
        'body': 'Pool Removed Successfully'
    }
