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

rds_client = zanolambdashelper.helpers.create_client('rds')
lambda_client = zanolambdashelper.helpers.create_client('lambda')

zanolambdashelper.helpers.set_logging('INFO')


def get_org_test_schedule(cursor, org_uuid):

    logging.info("Getting organisations monthly emergency device schedule...")
    sql = f"""SELECT deviceUUID, test_type_id,
            UNIX_TIMESTAMP(test_time) AS test_time
            FROM {database_dict['schema']}.{database_dict['emergency_test_schedule_table']}
            WHERE organisationUUID = %s """


    cursor.execute(sql, (org_uuid,))
    result = cursor.fetchall()

    if result:
        mapped_results = {}
        for row in result:
            device_uuid, test_type_id, test_time = row
            # Initialise list if key doesn't exist
            if test_type_id not in mapped_results:
                mapped_results[test_type_id] = []
            # Append device and time as a dict
            mapped_results[test_type_id].append({
                "deviceUUID": device_uuid,
                "test_time": int(test_time)
            })

        return mapped_results
    else:
        raise Exception(404,"No schedules set for organisation")


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        with conn.cursor() as cursor:
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor,
                                                                            database_dict['schema'],
                                                                            database_dict['users_table'],
                                                                            user_email)
            org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor,
                                                                               database_dict['schema'],
                                                                               database_dict[
                                                                                   'users_organisations_table'],
                                                                               user_uuid)

            # validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], user_uuid,
                                                        org_uuid)


            test_schedule = get_org_test_schedule(cursor, org_uuid)


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to get test schedule'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 404:  # if 422 then validation error
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
        'body': 'Obtained Org Test Schedule Successfully',
        'schedule': test_schedule,
    }
