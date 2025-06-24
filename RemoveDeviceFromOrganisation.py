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


def delete_device_from_organisation(cursor,  device_uuid, org_uuid, user_uuid):
    try:

        get_entry = f"""
                                              SELECT * FROM {database_dict['schema']}.{database_dict['devices_table']}
                                              WHERE deviceUUID = %s and organisationUUID = %s;
                              """
        cursor.execute(get_entry, (device_uuid, org_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        logging.info("Deleting device from organisation...")
        sql = f"""  
            DELETE d
            FROM {database_dict['devices_table']} d
            WHERE d.deviceUUID = %s AND d.organisationUUID = %s;
        """
        cursor.execute(sql, (device_uuid, org_uuid))

        sql_audit = sql % (device_uuid, org_uuid)

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['devices_table'], 2, device_uuid, sql_audit,
            historic_row_json, '{}', org_uuid, user_uuid
        )
    except Exception as e:
        logging.error(f"Error deleting device from organisation: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):
    try:

        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        device_uuid_raw = body_json.get('device_uuid')

        variables = {
            'device_uuid': {'value': device_uuid_raw['value'], 'value_type': 'uuid'},
        }

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
            zanolambdashelper.helpers.is_user_org_admin(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], user_uuid,
                                                        org_uuid)
            zanolambdashelper.helpers.is_target_device_in_org(cursor, database_dict['schema'],
                                                              database_dict['devices_table'], org_uuid,
                                                              device_uuid)
            delete_device_from_organisation(cursor, device_uuid, org_uuid, user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to remove device from organisation'
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
        'body': 'Device Removed From Organisation Successfully'
    }

