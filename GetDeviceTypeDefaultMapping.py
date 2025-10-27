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


def get_mapping_table(cursor):
    try:
        logging.info("Getting mapping details...")
        default_mapping_lookup_sql = f"""

    SELECT 
    COALESCE(e.device_type_ID, a.device_type_ID) AS device_type_ID,
    m.event_ID,
    m.action_ID,
    m.event_data,
    m.priority,
    m.sequence,
    m.time_days,
    m.time_start,
    m.time_stop,
    e.event_number,
    e.event_name,
    a.action_number,
    a.action_name
FROM device_type_default_mappings m
LEFT JOIN device_type_events e ON m.event_ID = e.event_ID
LEFT JOIN device_type_actions a ON m.action_ID = a.action_ID
ORDER BY device_type_ID, event_ID, action_ID;


        """
        cursor.execute(default_mapping_lookup_sql)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # Convert to dict keyed by device_type_ID
        mapping_dict = {}
        for row in result:
            device_type_id = row[0]
            row_dict = dict(zip(columns[1:], row[1:]))
            if device_type_id not in mapping_dict:
                mapping_dict[device_type_id] = []
            mapping_dict[device_type_id].append(row_dict)

        return mapping_dict

    except Exception as e:
        logging.error(f"Error fetching mapping table: {e}")
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

            mapping_table = get_mapping_table(cursor)

            output_dict = {
                "mapping_table": mapping_table,
            }

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to retrive device type default mappings table'
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




