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


def generate_code():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=6)).upper()


def append_invite(cursor, organisation_id, invite_type_id, target_email):
    #parse to int
    invite_type_id = int(invite_type_id)
    code = generate_code()
    max_attempts = 5
    attempt = 0
    logging.info("Generating Invite Code...")
    while attempt < max_attempts:
        attempt += 1
        if invite_type_id == 1 or invite_type_id == 3:
            delete_sql = f"DELETE FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE organisationID = %s AND inviteID = %s"
            cursor.execute(delete_sql, (organisation_id, invite_type_id))
            logging.info("Deleted existing invite code for the organization.")
            
            if invite_type_id == 1:
                sql = f"INSERT INTO {database_dict['schema']}.{database_dict['organisation_invites_table']} (invite_code, organisationID, target_email, inviteID, valid_until) " \
                      f"VALUES (%s, %s, NULL, %s, DATE_ADD(NOW(), INTERVAL 30 MINUTE))"
            else:
                sql = f"INSERT INTO {database_dict['schema']}.{database_dict['organisation_invites_table']} (invite_code, organisationID, target_email, inviteID) " \
                      f"VALUES (%s, %s, NULL, %s)"
            try:
                cursor.execute(sql, (code, organisation_id, invite_type_id))
                logging.info("Inserted new invite code into the table.")
                break
            except mysql.connector.IntegrityError as e:
                if e.errno == 1062:
                    logging.info(f"The code '{code}' already exists in the table. Retrying...")
                    code = generate_new_code()
                else:
                    logging.error(f"Error generating code: {e}")
                    traceback.print_exc()
                    raise Exception(400, e)
        else:
            logging.error(f"Unsupported invite type: {invite_type_id}")
            traceback.print_exc()
            raise Exception(400, e)
    else:
        logging.error(f"Maximum retry attempts reached. Unable to insert the invite.")
        traceback.print_exc()
        raise Exception(400, e)
    return code



def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
        conn.autocommit = False 
    
        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)


        invite_type_id_raw = body_json.get('invite_type_id')
        target_email_raw = body_json.get('target_email', None)
        
        variables = {
            'invite_type_id': {'value': invite_type_id_raw['value'], 'value_type': invite_type_id_raw['value_type']},
        }
        
        if target_email_raw: #add optionals if exists
            variables['target_email'] = {'value': target_email_raw['value'], 'value_type': target_email_raw['value_type']}
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        invite_type_id = variables['invite_type_id']['value']
        target_email = variables['target_email']['value'] if target_email_raw else None

        with conn.cursor() as cursor:
            login_user_id, user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id, org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
           
            logging.info("Appending new invite code...")
            generated_code = append_invite(cursor, organisation_id, invite_type_id, target_email)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation 
            body_value = e.args[1]
        else:
            body_value = 'Unable to generate invite'
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
        'body': 'Invite Generated Successfully',
        'code': generated_code
    }
