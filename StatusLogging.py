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

mailing_list = ['stuart.rose@zanocontrols.co.uk', 'mark.lewin@zanocontrols.co.uk',
                'darryl.schofield@zanocontrols.co.uk', 'Thomas.Lambert@zanocontrols.co.uk']
sender_email = 'noreply@zanocontrols.co.uk'

database_details = zanolambdashelper.helpers.get_db_details()

rds_host = database_details['rds_host']
rds_port = database_details['rds_port']
rds_db = database_details['rds_db']
rds_user = database_details['rds_user']
rds_region = database_details['rds_region']

database_dict = zanolambdashelper.helpers.get_database_dict()

rds_client = zanolambdashelper.helpers.create_client('rds')
ses_client = zanolambdashelper.helpers.create_client('ses')

lambda_client = zanolambdashelper.helpers.create_client('lambda')

zanolambdashelper.helpers.set_logging('INFO')

import boto3
from botocore.exceptions import ClientError
import logging


def send_device_status_email(org_uuid, organisation_name, device_details, status_details, mailing_list, sender_email,
                             subject):
    # Sends an email via Amazon SES listing statuses encountered on a device.

    # Unpack device details
    deviceUUID, device_type_ID, device_long_address, associated_hub, serial = device_details

    org_text = f"""
        Organisation Name: {organisation_name} 
        Organisation UUID: {org_uuid}
    """

    # Build status list as text
    status_lines = []
    for status_code, status_message, status_type_id in status_details:
        status_lines.append(
            f"        Code: {status_code}, Status Type: {status_type_id}, Status Message: {status_message}")

    status_text = "\n".join(status_lines)

    # Build device info text
    device_text = f"""
        Device UUID: {deviceUUID}
        Device Type ID: {device_type_ID}
        Device Long Address: {device_long_address or 'N/A'}
        Associated Hub UUID: {associated_hub or 'N/A'}
        Associated Hub Serial: {serial or 'N/A'}
    """

    # Full email body
    email_body = f"""
        {org_text}

        Status(s) encountered:

        {status_text}

        {device_text}
        """

    try:
        response = ses_client.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': mailing_list
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': email_body
                    }
                }
            }
        )
        logging.info(f"Email sent! Message ID: {response['MessageId']}")
    except ClientError as e:
        logging.error(f"Failed to send email: {e.response['Error']['Message']}")
        raise


def insert_device_status_log(cursor, org_uuid, organisation_name, device_details, status_details):
    # inserting device logs based on multiple statuses per device
    try:
        logging.info("Inserting device status log entries")
        sql = f"""
            INSERT INTO device_status_log (
                organisationUUID,
                organisation_name,
                deviceUUID,
                device_type_ID,
                device_long_address,
                associated_hubUUID,
                associated_hub_serial,
                status_code,
                status_message,
                status_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepare rows to insert: one row per status
        rows_to_insert = []
        for status in status_details:
            status_code, status_message, status_type_id = status
            deviceUUID, device_type_ID, device_long_address, associated_hub, serial = device_details
            rows_to_insert.append((
                org_uuid,
                organisation_name,
                deviceUUID,
                device_type_ID,
                device_long_address,
                associated_hub,
                serial,
                status_code,
                status_message,
                status_type_id
            ))

        # Execute bulk insert
        cursor.executemany(sql, rows_to_insert)

        logging.info(f"Inserted {len(rows_to_insert)} device status log rows")

    except Exception as e:
        logging.error(f"Error inserting device status log: {e}")
        traceback.print_exc()
        raise


def get_organisation_name(cursor, org_uuid):
    try:
        logging.info("Getting organisation name from organisation UUID")
        sql = f"""
            SELECT organisation_name
            FROM {database_dict['schema']}.{database_dict['organisations_table']}
            WHERE organisationUUID = %s
        """
        cursor.execute(sql, (org_uuid,))

        organisation_name = cursor.fetchone()

        if not organisation_name:  # if the details cannot be found
            raise Exception("No organisation found");

        return organisation_name

    except Exception as e:
        logging.error(f"Error getting organisation name: {e}")
        traceback.print_exc()
        raise


def get_status_details(cursor, status_codes):
    try:
        logging.info("Getting status details from status code")
        placeholders = ', '.join(['%s'] * len(status_codes))
        sql = f"""
            SELECT status_code, status_message, status_type_id
            FROM {database_dict['schema']}.{database_dict['status_lookup_table']}
            WHERE status_code IN ({placeholders})  
        """
        cursor.execute(sql, status_codes)

        status_details = cursor.fetchall()

        if not status_details:  # if the details cannot be found
            raise Exception("No status details found");

        return status_details

    except Exception as e:
        logging.error(f"Error getting status details: {e}")
        traceback.print_exc()
        raise


def get_device_details(cursor, device_uuid):
    try:
        logging.info("Getting status details from status code")
        sql = f"""
             SELECT a.deviceUUID, a.device_type_ID, a.long_address as device_long_address, a.associated_hub, b.serial
                FROM {database_dict['schema']}.{database_dict['devices_table']} a
                INNER JOIN {database_dict['schema']}.{database_dict['hubs_table']} b
                ON a.associated_hub = b.hubUUID
                WHERE a.deviceUUID = %s 

                UNION

                SELECT hubUUID as deviceUUID, device_type_ID, NULL as device_long_address, NULL as associated_hub, serial
                FROM {database_dict['schema']}.{database_dict['hubs_table']} 
                WHERE hubuuid = %s 
        """
        cursor.execute(sql, (device_uuid, device_uuid,))

        device_details = cursor.fetchone()

        if not device_details:  # if the details cannot be found
            raise Exception("No device details found");

        return device_details

    except Exception as e:
        logging.error(f"Error getting device details: {e}")
        traceback.print_exc()
        raise


def extract_topic_variables(topic):
    logging.info("Getting mqtt topic details")
    topic_split = topic.split('/')
    print(topic_split);

    # Return both halves as a tuple
    if len(topic_split) >= 2:
        return topic_split[0], topic_split[1]
    else:
        logging.error(f"Invalid topic structure....")
        raise


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

            status_details = get_status_details(cursor, status_codes)
            if all(status[2] == 1 for status in status_details):  # on ok status type dont send a progress
                return
            organisation_name, = get_organisation_name(cursor, org_uuid)
            device_details = get_device_details(cursor, device_uuid)
            insert_device_status_log(cursor, org_uuid, organisation_name, device_details, status_details)
            subject = f"{organisation_name} Device: {device_details[0]} Status Alert: {status_codes}"

            send_device_status_email(org_uuid, organisation_name, device_details, status_details, mailing_list,
                                     sender_email, subject)




    except Exception as e:
        logging.error(f"Internal Server Error: {e}")

        status_value = 500
        body_value = 'Unable to log status'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 403:  # if 422 then validation
                body_value = e.args[1]
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
        'body': 'Status Logged Successfully'
    }



