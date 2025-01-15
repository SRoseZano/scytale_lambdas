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

gateway_url = 'https://zuxtyllg91.execute-api.eu-west-2.amazonaws.com/Dev/download_hub_firmware/'

rds_host = database_details['rds_host']
rds_port = database_details['rds_port']
rds_db = database_details['rds_db']
rds_user = database_details['rds_user']
rds_region = database_details['rds_region']

database_dict = zanolambdashelper.helpers.get_database_dict()

rds_client =  zanolambdashelper.helpers.create_client('rds') 

zanolambdashelper.helpers.set_logging('INFO')

def check_firmware_version(cursor, hub_UUID):
    try:
        logging.info("Executing SQL query checking current firmware version to target ")
        
        sql = f"""
        
            SELECT 
                CASE 
                    WHEN current_firmware != target_firmware THEN target_firmware
                    ELSE NULL
                END AS result
            FROM {database_dict['schema']}.{database_dict['hubs_table']}
            WHERE hubUUID = %s;

        """
        cursor.execute(sql, (hub_UUID,))
        hub_firmware_result = cursor.fetchall()
        if hub_firmware_result:
            target_firmware = hub_firmware_result[0]
            if target_firmware[0] is None:
                return None
            else:
                return target_firmware[0]
        else:
            return None
        
    except Exception as e:
        logging.error(f"Error obtaining hub firmware: {e}")
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
        hub_uuid_raw = body_json.get('hub_UUID')
        
        variables = {
            'hub_UUID': {'value': hub_uuid_raw['value'], 'value_type': hub_uuid_raw['value_type']},
        }
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        hub_uuid = variables['hub_UUID']['value']

        with conn.cursor() as cursor:
                login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
                organisation_id = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
                zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
                
                target_firmware = check_firmware_version(cursor, hub_uuid)
                
                print(target_firmware)
                
                if target_firmware is not None:
                    output_dict = {'update': 1, 'target_firmware_version': gateway_url+target_firmware}
                else:
                    output_dict = {'update': 0}

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to retrive hub firmware details'
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

    return {'statusCode': 200, 'body': output_dict}
