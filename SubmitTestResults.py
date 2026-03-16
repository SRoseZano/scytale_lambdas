import boto3
import json
from datetime import datetime, timezone
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

zanolambdashelper.helpers.set_logging('INFO')

def add_monthly_test_result(cursor, org_uuid, device_uuid, result, result_time_since_epoch):
    logging.info("Inserting monthly test result...")

    sql = f"""INSERT INTO {database_dict['schema']}.{database_dict['emergency_functional_test_result_table']} (organisationUUID, deviceUUID, result, result_timestamp) 
            VALUES (%s,%s,%s,%s)

    """
    cursor.execute(sql, (org_uuid, device_uuid, result, datetime.fromtimestamp(result_time_since_epoch, tz=timezone.utc)))

def add_yearly_test_result(cursor, org_uuid, device_uuid, result, result_time_since_epoch):
    logging.info("Inserting yearly test result...")

    sql = f"""INSERT INTO {database_dict['schema']}.{database_dict['emergency_discharge_test_result_table']} (organisationUUID, deviceUUID, result, result_timestamp) 
            VALUES (%s,%s,%s,%s)

    """
    cursor.execute(sql, (org_uuid, device_uuid, result, datetime.fromtimestamp(result_time_since_epoch, tz=timezone.utc)))

def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        test_type_id = body_json.get('test_type_id')
        device_uuid = body_json.get('device_uuid')
        result = body_json.get('result')
        result_time_since_epoch = body_json.get('result_time')

        if None in (test_type_id, device_uuid, result, result_time_since_epoch):
            raise Exception(410, "Missing arguments")

        variables = {
            'device_uuid': {'value': device_uuid, 'value_type': 'uuid'}
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        device_uuid = variables['device_uuid']['value']

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


            if int(test_type_id) == 1:
                add_monthly_test_result(cursor,org_uuid,device_uuid,result,result_time_since_epoch)
            elif int(test_type_id) == 2:
                add_yearly_test_result(cursor, org_uuid, device_uuid, result, result_time_since_epoch)
            else:
                raise Exception("Invalid test type id")

            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to submit results'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 410:  # if 422 then validation error
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
        'body': 'Submitted Test Results Successfully'
    }