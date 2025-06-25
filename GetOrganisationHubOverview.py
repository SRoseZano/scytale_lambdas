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
print("gotten the details")

database_dict = zanolambdashelper.helpers.get_database_dict()

print("gotten the dict")

rds_client = zanolambdashelper.helpers.create_client('rds')

print("gotten the client")

zanolambdashelper.helpers.set_logging('INFO')


def get_organisation_details(cursor, user_uuid):
    try:
        logging.info("Getting organisation details...")
        organisation_details_sql = f"""
            SELECT DISTINCT a.organisationUUID, b.permissionid FROM {database_dict['schema']}.{database_dict['organisations_table']} a 
            JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b 
            ON a.organisationUUID = b.organisationUUID
            AND b.userUUID = '{user_uuid}'
            LIMIT 1
        """
        cursor.execute(organisation_details_sql)
        organisation_details_result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        if organisation_details_result:
            organisation_details_result_list = dict(zip(columns, organisation_details_result[0]))

            return organisation_details_result_list
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching organisation details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_organisation_users(cursor, org_uuid, organisation_details):
    try:
        logging.info("Getting organisation users...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            organisation_users_sql = f"""
                SELECT DISTINCT a.userUUID, a.email, b.permissionid
                FROM {database_dict['schema']}.{database_dict['users_table']} a
                JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b 
                ON a.userUUID = b.userUUID
                AND a.hub_user = 0
                AND b.organisationUUID = %s
            """
            cursor.execute(organisation_users_sql, (org_uuid,))
            organisation_users_result = cursor.fetchall()

            if organisation_users_result:
                # Generate integer-based IDs for users
                user_uuid_to_id = {user[0]: idx + 1 for idx, user in enumerate(organisation_users_result)}

                users_dict = {
                    user_uuid_to_id[user[0]]: {
                        "Details": {
                            "userUUID": user[0],
                            "email": user[1],
                            "permissionid": user[2]
                        }
                    }
                    for user in organisation_users_result
                }
                return users_dict, user_uuid_to_id  # Return both dictionary & UUID-to-ID mapping
            else:
                return {}, {}
        else:
            return {}, {}
    except Exception as e:
        logging.error(f"Error fetching organisation users: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_organisation_invite_code(cursor, org_uuid, organisation_details):
    try:
        logging.info("Getting organisation invite codes...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            organisation_invite_code_sql = current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            organisation_invite_code_sql = f"""SELECT DISTINCT invite_code 
                                                FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} 
                                                WHERE organisationUUID = '{org_uuid}'
                                                AND valid_until >= NOW()
                                                AND inviteID = 1
                                                LIMIT 1"""

            cursor.execute(organisation_invite_code_sql)
            organisation_invite_code_result = cursor.fetchone()
            if organisation_invite_code_result:
                organisation_invite_code = organisation_invite_code_result[0]
                return organisation_invite_code
            else:
                return None
        else:
            return None
    except Exception as e:
        logging.error(f"Error fetching organisation invite codes: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def merge_pools_users_devices(pool_details, pools_devices):
    try:
        logging.info("Merging pools, users, and devices...")
        merged_data = {}
        for pool_id, details in pool_details.items():
            pool_uuid = details['Details']['poolUUID']
            merged_data[pool_id] = details.copy()  # Copy pool details
            devices = pools_devices.get(pool_uuid, {}).get('Devices', [])  # List of device IDs
            merged_data[pool_id]['Devices'] = devices
        return merged_data
    except Exception as e:
        logging.error(f"Error merging pools, users, and devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_pool_details(cursor, org_uuid):
    try:
        logging.info("Getting pool details...")
        pools_sql = f"""
            SELECT DISTINCT a.poolUUID, a.pool_name, a.parentUUID
            FROM {database_dict['schema']}.{database_dict['pools_table']} a 
            WHERE a.organisationUUID = '{org_uuid}'
        """
        cursor.execute(pools_sql)
        pools_result = cursor.fetchall()

        if pools_result:
            pool_uuids = {pool[0] for pool in pools_result}

            # Create a mapping of UUIDs to sequential integers
            uuid_to_int = {uuid: idx + 1 for idx, uuid in enumerate(pool_uuids)}

            processed_result = [(uuid_to_int[pool[0]], pool[0], pool[1], uuid_to_int.get(pool[2], None)) for pool in
                                pools_result]
            pools_details = {pool[0]: {'Details': {'poolUUID': pool[1], 'pool_name': pool[2], 'parentUUID': pool[3]}}
                             for pool in processed_result}
            return pools_details
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching pool details: {e}")
        traceback.print_exc()
        raise Exception(400, e)




def get_device_details(cursor, org_uuid, organisation_details, hub_uuid_to_id):
    try:
        logging.info("Getting device details...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            devices_details_sql = f"""
                SELECT DISTINCT a.deviceUUID, a.long_address, a.short_address,  a.device_name, a.registrant, a.device_type_id, a.associated_hub
                FROM {database_dict['schema']}.{database_dict['devices_table']} a 
                WHERE organisationUUID = '{org_uuid}'
            """
            cursor.execute(devices_details_sql)
            devices_details_result = cursor.fetchall()

            if devices_details_result:
                # Generate integer-based IDs for devices
                device_uuid_to_id = {device[0]: idx + 1 for idx, device in enumerate(devices_details_result)}
                devices_dict = {
                    device_uuid_to_id[device[0]]: {
                        "Details": {
                            "deviceUUID": device[0],
                            "long_address": device[1],
                            "short_address": device[2],
                            "device_name": device[3],
                            "registrant": device[4],
                            "device_type_id": device[5],
                            "associated_hub": hub_uuid_to_id.get(device[6])  # Convert UUID to integer ID
                        }
                    }
                    for device in devices_details_result
                }
                return devices_dict, device_uuid_to_id  # Return dictionary & UUID-to-ID mapping
            else:
                return {}, {}
    except Exception as e:
        logging.error(f"Error fetching device details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_pools_devices(cursor, device_uuid_to_id):
    try:
        logging.info("Getting pools and associated devices...")
        if not device_uuid_to_id:
            return {}

        device_uuids = list(device_uuid_to_id.keys())
        placeholders = ",".join(["%s"] * len(device_uuids))

        devices_pools_sql = f"""
            SELECT DISTINCT a.deviceUUID, a.poolUUID
            FROM {database_dict['schema']}.{database_dict['pools_devices_table']} a
            WHERE a.deviceUUID IN ({placeholders})
        """
        cursor.execute(devices_pools_sql, tuple(device_uuids))
        devices_pools_result = cursor.fetchall()

        pool_devices_dict = {}
        for device_uuid, pool_uuid in devices_pools_result:
            device_id = device_uuid_to_id.get(device_uuid)  # Convert UUID to integer ID
            if device_id:
                if pool_uuid not in pool_devices_dict:
                    pool_devices_dict[pool_uuid] = {'Devices': [device_id]}
                else:
                    pool_devices_dict[pool_uuid]['Devices'].append(device_id)

        return pool_devices_dict
    except Exception as e:
        logging.error(f"Error fetching pools and associated devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_hub_details(cursor, org_uuid, organisation_details):
    try:
        logging.info("Getting hub details...")

        if not organisation_details:
            return {}, {}

        hub_details_sql = f"""
            SELECT DISTINCT a.hubUUID, a.serial, a.hub_name, a.registrant, a.device_type_id,
                            b.long_address, b.short_address
            FROM {database_dict['schema']}.{database_dict['hubs_table']} a
            LEFT JOIN {database_dict['schema']}.{database_dict['hub_radios_table']} b
            ON a.hubUUID = b.hubUUID AND a.organisationUUID = %s
        """
        cursor.execute(hub_details_sql, (org_uuid,))
        hub_details_result = cursor.fetchall()

        if not hub_details_result:
            return {}, {}

        # Group radios by hubUUID
        hub_map = {}
        for row in hub_details_result:
            hub_uuid, serial, hub_name, registrant, device_type_id, long_addr, short_addr = row

            if hub_uuid not in hub_map:
                hub_map[hub_uuid] = {
                    'hubUUID': hub_uuid,
                    'serial': serial,
                    'hub_name': hub_name,
                    'registrant': registrant,
                    'device_type_id': device_type_id,
                    'radios': []
                }

            if long_addr and short_addr:
                hub_map[hub_uuid]['radios'].append({
                    'long_addr': long_addr,
                    'short_addr': short_addr
                })
        hub_uuid_to_id = {uuid: idx + 1 for idx, uuid in enumerate(hub_map.keys())}
        hub_details = {
            str(idx + 1): {'Details': data}
            for idx, (_, data) in enumerate(hub_map.items())
        }

        return hub_details, hub_uuid_to_id

    except Exception as e:
        logging.error(f"Error fetching hub details: {e}")
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

        with conn.cursor() as cursor:
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'],
                                                                            database_dict['users_table'], user_email)
            organisation_details = get_organisation_details(cursor, user_uuid)
            if organisation_details:
                organisation_uuid = organisation_details['organisationUUID']
                hub_details, hub_uuid_to_id = get_hub_details(cursor, organisation_uuid, organisation_details)
                device_details, device_uuid_to_id = get_device_details(cursor, organisation_uuid,
                                                                       organisation_details, hub_uuid_to_id)
                pools_details = get_pool_details(cursor, organisation_uuid)
                pools_devices = get_pools_devices(cursor, device_uuid_to_id)
                pools_merged = merge_pools_users_devices(pools_details, pools_devices)

                output_dict = {
                    "organisationInfo": organisation_details,
                    "Pools": pools_merged,
                    "Devices": device_details,
                    "Hubs": hub_details
                }
            else:
                output_dict = {}

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to retrieve organisation details'
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

    if not output_dict:
        return {'statusCode': 204}
    else:
        return {'statusCode': 200, 'body': output_dict}




