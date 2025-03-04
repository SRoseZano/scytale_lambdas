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

print("imported")

database_details = zanolambdashelper.helpers.get_db_details()

rds_host = database_details['rds_host']
rds_port = database_details['rds_port']
rds_db = database_details['rds_db']
rds_user = database_details['rds_user']
rds_region = database_details['rds_region']

print(rds_host)

database_dict = zanolambdashelper.helpers.get_database_dict()

rds_client = zanolambdashelper.helpers.create_client('rds')

zanolambdashelper.helpers.set_logging('INFO')


def get_status_table(cursor):
    try:
        logging.info("Getting organisation details...")
        status_lookup_sql = f"""
            SELECT DISTINCT a.* FROM {database_dict['schema']}.{database_dict['status_lookup_table']} a 
        """
        cursor.execute(status_lookup_sql)
        status_lookup_result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        if status_lookup_result:
            status_lookup_result_list = dict(zip(columns, status_lookup_result))

            return status_lookup_result_list
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching status table: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)
        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['params']['querystring']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        with (conn.cursor() as cursor):

            status_table = get_status_table(cursor)

            output_dict = {
                "status_table": status_table,
            }

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to retrive status message table'
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




