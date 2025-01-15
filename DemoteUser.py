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

def can_user_be_demoted(cursor, organisation_id, user_id, target_user_id):
    try:
        logging.info("Executing SQL query to check if user can be demoted")

        sql = f"""
            SELECT permissionid
            FROM {database_dict['schema']}.{database_dict['users_organisations_table']}
            WHERE organisationID = %s AND userID = %s;

        """

        cursor.execute(sql, (organisation_id, user_id))

        user_result = cursor.fetchone()

        sql = f"""
            SELECT permissionid
            FROM {database_dict['schema']}.{database_dict['users_organisations_table']}
            WHERE organisationID = %s AND userID = %s;

        """

        cursor.execute(sql, (organisation_id, target_user_id))

        target_user_result = cursor.fetchone()

        if user_result[0] < target_user_result[0]: # if permissions of user is higher than target
            return True
        else:
            raise ValueError("You do not have permissions to demote target user")

    except Exception as e:
        logging.error(f"Error checking if user can be demoted: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def demote_user(cursor, organisation_id, user_id):
    try:
        logging.info("Executing SQL query to demote user to admin")
        # SQL query to find top level pool and assign to everyone under it
        sql = f"""
            UPDATE {database_dict['schema']}.{database_dict['users_organisations_table']}
            SET permissionID = 3
            WHERE organisationID = %s AND userID = %s;

        """

        cursor.execute(sql, (organisation_id, user_id))
    
    except Exception as e:
        logging.error(f"Error demoting user: {e}")
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

        # Extract relevant attributes
        user_id_raw = body_json.get('user_id')
        
        variables = {
            'user_id': {'value': user_id_raw['value'], 'value_type': user_id_raw['value_type']},
        }
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        user_id = variables['user_id']['value']


        with conn.cursor() as cursor:
            login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            zanolambdashelper.helpers.is_target_user_in_org(cursor,database_dict['schema'],database_dict['users_organisations_table'], organisation_id, user_id)
            can_user_be_demoted(cursor, organisation_id, login_user_id,user_id)
            demote_user(cursor, organisation_id, user_id)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation 
            body_value = e.args[1]
        else:
            body_value = 'Unable to demote user'
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
        'body': 'User Demoted Successfully'
    }