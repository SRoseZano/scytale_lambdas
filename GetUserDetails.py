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



def get_user_details(cursor, user_id):
    
    try:
        logging.info("Getting user details...")
        sql = f"""
            SELECT  userid, userUUID, email, first_name, last_name, DATE_FORMAT(birthdate, '%m/%d/%Y') AS birthdate,
                     zone_info, locale
            FROM {database_dict['schema']}.{database_dict['users_table']} 
            WHERE userid = %s 
        """
        cursor.execute(sql, (user_id,))
        result = cursor.fetchone()
    
        columns = [desc[0] for desc in cursor.description]
    
        if result:
            result_dict = dict(zip(columns, result))
            return result_dict
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching user details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):

    database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)

    conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
    conn.autocommit = False

    auth_token = event['params']['header']['Authorization']
    user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

    try:
        with conn.cursor() as cursor:
            login_user_id, user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            user_details = get_user_details(cursor, login_user_id)


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to retrive organisation details'
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
        'body': user_details
    }
