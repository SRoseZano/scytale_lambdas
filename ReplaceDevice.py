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

zanolambdashelper.helpers.set_logging('INFO')

max_org_devices = 500




def get_org_device_count(cursor, org_uuid):
    try:
        logging.info("Fetching org device count...")
        sql = f"SELECT COUNT(DISTINCT deviceUUID) FROM {database_dict['schema']}.{database_dict['devices_table']} WHERE organisationUUID = %s"
        cursor.execute(sql, (org_uuid,))
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error fetching org device count: {e}")
        traceback.print_exc()
        raise Exception(400, e)



def update_device(cursor, long_address, associated_hub, user_email, device_uuid,
                   org_uuid, user_uuid):
    try:
        get_entry = f"""
                            SELECT * FROM {database_dict['schema']}.{database_dict['devices_table']}
                            WHERE organisationUUID = %s AND deviceUUID = %s;
                            """
        cursor.execute(get_entry, (org_uuid, device_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        logging.info("Updating device entry...")
        sql = f"UPDATE {database_dict['schema']}.{database_dict['devices_table']} SET long_address = %s, associated_hub = %s, registrant = %s  WHERE organisationUUID = %s AND deviceUUID = %s "
        cursor.execute(sql, (long_address,associated_hub, user_email, org_uuid, device_uuid,))

        sql_audit = sql % (
        long_address,associated_hub, user_email, org_uuid, device_uuid)


        get_entry = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['devices_table']}
                        WHERE deviceUUID = %s;
        """
        cursor.execute(get_entry, (device_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['devices_table'], 3, org_uuid, sql_audit,
            historic_row_json, current_row_json, org_uuid, user_uuid
        )

    except Exception as e:
        logging.error(f"Error creating device entry: {e}")
        traceback.print_exc()
        raise Exception(400, e) from e





def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        # Extract relevant attributes if non existant set empty

        long_address_raw = body_json.get('long_address')
        device_uuid_raw = body_json.get('device_uuid')
        associated_hub_raw = body_json.get('associated_hub')

        variables = {
            'long_address': {'value': long_address_raw['value'], 'value_type': 'long_address'},
            'device_uuid': {'value': device_uuid_raw['value'], 'value_type': 'uuid'},
            'associated_hub': {'value': associated_hub_raw['value'], 'value_type': 'uuid'},

        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        long_address = variables['long_address']['value']
        associated_hub = variables['associated_hub']['value']
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


            update_device(cursor, long_address,  associated_hub, user_email,
                                      device_uuid, org_uuid, user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = 500
        body_value = 'Unable to update device'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422:  # if 422 then validation error
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
        'body': 'Device Updated Successfully'
    }
