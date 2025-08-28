import json
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
from botocore.exceptions import ClientError
import json
import firebase_admin
from firebase_admin import credentials, messaging
import os

firebase_messenger_lambda = "FirebaseMessenger"

database_details = zanolambdashelper.helpers.get_db_details()

rds_host = database_details['rds_host']
rds_port = database_details['rds_port']
rds_db = database_details['rds_db']
rds_user = database_details['rds_user']
rds_region = database_details['rds_region']

database_dict = zanolambdashelper.helpers.get_database_dict()

rds_client = zanolambdashelper.helpers.create_client('rds')

lambda_client = zanolambdashelper.helpers.create_client('lambda')

zanolambdashelper.helpers.set_logging('INFO')


def get_device_details(cursor, device_uuid):
    try:
        logging.info("Getting device details from UUID")
        sql = f"""
            SELECT deviceUUID, device_name, device_type_ID 
            FROM {database_dict['schema']}.{database_dict['devices_table']}
            WHERE deviceUUID = %s
            LIMIT 1
        """
        cursor.execute(sql, (device_uuid,))

        device_details = cursor.fetchone()

        if not device_details:  # if details do not exist for device topic then its probably a hub
            sql = f"""
                SELECT hubUUID, hub_name, device_type_ID 
                FROM {database_dict['schema']}.{database_dict['hubs_table']}
                WHERE hubUUID = %s
                LIMIT 1
            """
            cursor.execute(sql, (device_uuid,))

            device_details = cursor.fetchone()

        if not device_details:  # if the details cannot be found in either device or hub table
            raise Exception("Device details not found for given UUID");

        return device_details

    except Exception as e:
        logging.error(f"Error getting device details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_status_type_id(cursor, status_code):
    try:
        logging.info("Getting status_type_id from status code")
        sql = f"""
            SELECT status_type_id
            FROM {database_dict['schema']}.{database_dict['status_lookup_table']}
            WHERE status_code = %s
            LIMIT 1
        """
        cursor.execute(sql, (status_code,))

        status_type_id = cursor.fetchone()

        if not status_type_id:  # if the details cannot be found in either device or hub table
            raise Exception("Status type id not found for given status code");

        return status_type_id[0]

    except Exception as e:
        logging.error(f"Error getting status_type_id: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def extract_topic_variables(topic):
    logging.info("Getting mqtt topic details")
    topic_split = topic.split('/')
    print(topic_split);

    # Return both halves as a tuple
    if len(topic_split) >= 2:
        return topic_split[0], topic_split[1]
    else:
        logging.error(f"Invalid topic structure....")
        raise Exception(400, "Invalid topic structure....")


def get_highest_priority_alert(status_codes):
    logging.info("Getting highest priority alert code")
    if isinstance(status_codes, list):
        # If status_codes is a list of integers, get the max
        status = max(status_codes)
        return status
    else:
        logging.error(f"Provided status codes are not a list....")
        raise Exception(400, "Provided status codes are not a list....")


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False
        status_codes = event.get('status')
        mqtt_topic = event.get('mqtt_topic')
        org_uuid, device_uuid = extract_topic_variables(mqtt_topic)
        with conn.cursor() as cursor:
            status_code = get_highest_priority_alert(status_codes)
            status_code_type_id = get_status_type_id(cursor, status_code)
            print(status_code_type_id)
            if status_code_type_id == 1:  # on ok status type dont send a message
                return
            deviceid, device_name, device_type_ID = get_device_details(cursor, device_uuid)

            # Run policy creation lambda
            response = lambda_client.invoke(
                FunctionName=firebase_messenger_lambda,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=json.dumps({"topic": f"{org_uuid}_{device_uuid}", "status_code_type_id": status_code_type_id,
                                    "device_uuid": device_uuid, "device_name": device_name,
                                    "device_type_ID": device_type_ID})
            )
            logging.info("Message Requested")

            response_payload = response['Payload'].read().decode('utf-8')
            logging.info(response_payload)

            if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
                logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
                traceback.print_exc()
                raise Exception(400, {response_payload})

            logging.info("Message Sent")

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 403:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to send message'
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
        'body': 'Message Sent Successfully'
    }



