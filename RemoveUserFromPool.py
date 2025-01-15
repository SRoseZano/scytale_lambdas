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

database_dict['schema'] = "zanocontrols"
database_dict['users_organisations_table'] = "users_organisations"
database_dict['pools_users_table'] = "pools_users"
database_dict['users_table'] = "users"
database_dict['pools_table'] = "pools"


zanolambdashelper.helpers.set_logging('INFO')

def has_permissions_to_remove_target(cursor, login_user_id, user_id, organisation_id):
    try:
        
        logging.info("Checking login user permissions...")

        sql = f"""
            SELECT DISTINCT permissionid
            FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
            WHERE a.userid = %s
            AND a.organisationid = %s
            AND a.permissionid 
            LIMIT 1
        """

        cursor.execute(sql, (login_user_id, organisation_id))
        login_user_permissions = cursor.fetchone()
        
        logging.info("Checking target user permissions...")

        sql = f"""
            SELECT DISTINCT permissionid
            FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
            WHERE a.userid = %s
            AND a.organisationid = %s
            AND a.permissionid 
            LIMIT 1
        """

        cursor.execute(sql, (user_id, organisation_id))
        target_user_permissions = cursor.fetchone()

    except Exception as e:
        logging.error(f"Error checking user permissions: {e}")
        traceback.print_exc()
        raise Exception(400, e)

    if login_user_permissions[0] >= target_user_permissions[0]:
        print(login_user_permissions[0])
        print(target_user_permissions[0])
        raise Exception(402, "Cannot remove a user of same permission status from group, please demote user first")

def remove_user_from_pool(cursor, pool_id, user_id):
    try:
        logging.info("Executing SQL query to append device to pool:")
        logging.info(pool_id)
        # SQL query to add device to pool and all its children NOT NULL check to exclude trying to add NULL parent to table
        sql = f"""
            DELETE FROM {database_dict['schema']}.{database_dict['pools_users_table']}
                WHERE poolid IN (
                    WITH RECURSIVE PoolHierarchy AS (
                        SELECT parentid, poolID
                        FROM {database_dict['schema']}.{database_dict['pools_table']}
                        WHERE poolID = %s
                        UNION
                        SELECT p.parentid, p.poolID
                        FROM {database_dict['schema']}.{database_dict['pools_table']} p
                        JOIN PoolHierarchy ph ON p.parentID = ph.poolID
                    )
                    SELECT poolID FROM PoolHierarchy
                ) AND userid = %s;
            """
        
        cursor.execute(sql, (pool_id, user_id))
    
    except Exception as e:
        logging.error(f"Error removing user from pool: {e}")
        traceback.print_exc()
        raise Exception(400, e) from e



def lambda_handler(event, context):
    try:
        
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        # Extract relevant attributes
        user_id_raw = body_json.get('user_id')
        pool_id_raw = body_json.get('pool_id')
        
        variables = {
            'user_id': {'value': user_id_raw['value'], 'value_type': user_id_raw['value_type']},
            'pool_id': {'value': pool_id_raw['value'], 'value_type': pool_id_raw['value_type']}
        }
                
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)
        
        user_id = variables['user_id']['value']
        pool_id = variables['pool_id']['value']
    
        with conn.cursor() as cursor:
            login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            zanolambdashelper.helpers.is_target_user_in_org(cursor,database_dict['schema'],database_dict['users_organisations_table'], organisation_id, user_id)
            zanolambdashelper.helpers.is_target_pool_in_org(cursor,database_dict['schema'],database_dict['pools_table'], organisation_id, pool_id)
            
            has_permissions_to_remove_target(cursor,login_user_id, user_id,organisation_id)
            remove_user_from_pool(cursor, pool_id, user_id)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 402: # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to remove user from pool'
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
        'body': 'User Removed To Pool Successfully'
    }
