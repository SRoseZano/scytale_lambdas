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


def generate_unique_short_address(cursor, org_uuid):
    existing_short_addresses_query = f"""
        SELECT DISTINCT a.short_address
        FROM {database_dict['schema']}.{database_dict['hub_radios_table']} a
        JOIN {database_dict['schema']}.{database_dict['hubs_table']} b
        ON a.hubUUID = b.hubUUID
        WHERE b.organisationUUID = %s
    """
    cursor.execute(existing_short_addresses_query, (org_uuid,))
    existing_short_addresses = set(row[0].upper() for row in cursor.fetchall())

    attempt = 0
    while True:
        # Generate random number between 0 and 65535, format as 4-digit hex (uppercase)
        short_address = format(random.randint(0, 65535), '04X')
        if short_address not in existing_short_addresses:
            return short_address
        attempt += 1
        if attempt > 10000:
            raise Exception("Unable to generate a unique short address after many attempts.")




def add_radio_entry(cursor, user_uuid, org_uuid, hub_uuid, long_address):
    try:
        logging.info("Executing SQL query to relate radio addresses to hubs...")

        # Ensure short_address is unique within the same org
        short_address = generate_unique_short_address(cursor, org_uuid)

        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['hub_radios_table']} (hubUUID, long_address, short_address)
            VALUES (%s, %s, %s)
        """
        cursor.execute(sql, (hub_uuid, long_address, short_address))

        sql_audit = sql % (hub_uuid, long_address, short_address)

        get_current_entry = f"""
            SELECT * FROM {database_dict['schema']}.{database_dict['hub_radios_table']}
            WHERE hubUUID = %s AND long_address = %s AND short_address = %s
        """
        cursor.execute(get_current_entry, (hub_uuid, long_address, short_address))
        last_inserted_row = cursor.fetchall()

        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Initial row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['hub_radios_table'], 3, user_uuid, sql_audit,
            '{}', current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error adding radio addresses to hub: {e}")
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

        # Extract relevant attributes
        hub_uuid_raw = body_json.get('hub_UUID', '')
        long_address_raw = body_json.get('long_addr', '')

        variables = {
            'hub_UUID': {'value': hub_uuid_raw['value'], 'value_type': 'uuid'},
            'long_address': {'value': long_address_raw['value'], 'value_type': 'long_address'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        hub_uuid = variables['hub_UUID']['value']
        long_address = variables['long_address']['value']

        with conn.cursor() as cursor:
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'],
                                                                       database_dict['users_table'], user_email)
            org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor, database_dict['schema'],
                                                                       database_dict['users_organisations_table'],
                                                                       user_uuid)
            zanolambdashelper.helpers.is_user_org_admin(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], user_uuid, org_uuid)

            add_radio_entry(cursor,user_uuid, org_uuid, hub_uuid, long_address)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to add radio addresses to hub'
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

    return {'statusCode': 200, 'body': 'Radio addresses added to hub successfully '}
