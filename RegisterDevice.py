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


def generate_unique_short_address(cursor, org_uuid):
    existing_short_addresses_query = f"""
        SELECT DISTINCT a.short_address
        FROM {database_dict['schema']}.{database_dict['devices_table']} a
        WHERE a.organisationUUID = %s
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


def get_default_pool_id(cursor, org_uuid):
    try:
        logging.info("Fetching default pool UUID...")
        sql = f"SELECT poolUUID FROM {database_dict['schema']}.{database_dict['pools_table']} WHERE organisationUUID = %s and parentUUID is null"
        cursor.execute(sql, (org_uuid,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError("Unable to gather default pool")
    except Exception as e:
        logging.error(f"Error fetching default pool UUID: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_device(cursor, long_address, short_address, device_type_id, associated_hub, user_email, device_name,
                   org_uuid, user_uuid):
    try:
        logging.info("Creating device entry...")
        device_uuid = zanolambdashelper.helpers.generate_time_based_uuid(user_uuid, device_name)
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['devices_table']} (deviceUUID, long_address, short_address, device_type_id, associated_hub, registrant, device_name, organisationUUID) \
                VALUES (%s,%s, %s, %s, %s,%s, %s, %s)"
        cursor.execute(sql, (
        device_uuid, long_address, short_address, device_type_id, associated_hub, user_email, device_name, org_uuid))

        sql_audit = sql % (
        device_uuid, long_address, short_address, device_type_id, associated_hub, user_email, device_name, org_uuid)


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
            '{}', current_row_json, org_uuid, user_uuid
        )

        return device_uuid
    except Exception as e:
        logging.error(f"Error creating device entry: {e}")
        traceback.print_exc()
        raise Exception(400, e) from e


def add_device_to_default_pool(cursor, pool_uuid, device_uuid, org_uuid, user_uuid):
    try:
        logging.info("Adding device to default pool...")
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['pools_devices_table']} (poolUUID, deviceUUID) VALUES (%s, %s)"
        cursor.execute(sql, (pool_uuid, device_uuid))
        sql_audit = sql % (pool_uuid, device_uuid)

        get_entry = f"""
                    SELECT * FROM {database_dict['schema']}.{database_dict['pools_devices_table']}
                    WHERE deviceUUID = %s and poolUUID = %s;
                """
        cursor.execute(get_entry, (device_uuid, pool_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['pools_devices_table'], 3, pool_uuid, sql_audit,
            '{}', current_row_json, org_uuid, user_uuid
        )

    except Exception as e:
        logging.error(f"Error adding device to default pool: {e}")
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

        # Extract relevant attributes if non existant set empty

        device_name_raw = body_json.get('device_name')
        long_address_raw = body_json.get('long_address')
        device_type_id_raw = body_json.get('device_type_id')
        associated_hub_raw = body_json.get('associated_hub')

        variables = {
            'device_name': {'value': device_name_raw['value'], 'value_type': 'string_input'},
            'long_address': {'value': long_address_raw['value'], 'value_type': 'long_address'},
            'device_type_id': {'value': device_type_id_raw['value'], 'value_type': 'id'},
            'associated_hub': {'value': associated_hub_raw['value'], 'value_type': 'mac_address'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        device_name = variables['device_name']['value']
        long_address = variables['long_address']['value']
        device_type_id = variables['device_type_id']['value']
        associated_hub = variables['associated_hub']['value']

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

            org_device_count = get_org_device_count(cursor, org_uuid)
            if org_device_count + 1 > max_org_devices:  # if device count with new device is greater max then raise custom exception
                logging.error("Org is at device limit...")
                raise Exception(403, f"You have reached your organisations device limit of {max_org_devices}")

            short_address = generate_unique_short_address(cursor, org_uuid)
            device_uuid = create_device(cursor, long_address, short_address, device_type_id, associated_hub, user_email,
                                      device_name, org_uuid, user_uuid)
            pool_uuid = get_default_pool_id(cursor, org_uuid)
            add_device_to_default_pool(cursor, pool_uuid, device_uuid, org_uuid, user_uuid)
            device_topic = device_uuid
            conn.commit()


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 403:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to register device'
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
        'body': 'Device Added Successfully',
        'device_topic': device_topic,
        'short_addr': short_address
    }
