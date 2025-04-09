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
            SELECT DISTINCT a.*, b.permissionid FROM {database_dict['schema']}.{database_dict['organisations_table']} a 
            JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b 
            ON a.organisationUUID = b.organisationUUID
            AND b.userUUID = '{user_uuid}'
            LIMIT 1
        """

        print(organisation_details_sql)
        cursor.execute(organisation_details_sql)
        organisation_details_result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        if organisation_details_result:
            organisation_details_result_list = dict(zip(columns, organisation_details_result[0]))
            # Convert datetime object to string
            organisation_details_result_list['updated_at'] = organisation_details_result_list['updated_at'].isoformat()

            return organisation_details_result_list
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching organisation details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_organisation_users(cursor, organisation_uuid, organisation_details):
    try:
        logging.info("Getting organisation users...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            organisation_users_sql = f"""
                SELECT DISTINCT a.userUUID, a.email, b.permissionid
                FROM {database_dict['schema']}.{database_dict['users_table']} a
                JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b 
                ON a.userUUID = b.userUUID
                AND a.hub_user = 0
                AND b.organisationUUID = '{organisation_uuid}'
            """
            cursor.execute(organisation_users_sql)
            organisation_users_result = cursor.fetchall()
            # Convert the raw users output to a dictionary with userid as the key
            organisation_users = {user[0]: {'email': user[1], 'permissionid': user[2]} for user in
                                  organisation_users_result}
            return organisation_users
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching organisation users: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_organisation_invite_code(cursor, organisation_uuid, organisation_details):
    try:
        logging.info("Getting organisation invite codes...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            organisation_invite_code_sql = current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            organisation_invite_code_sql = f"""SELECT DISTINCT invite_code 
                                                FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} 
                                                WHERE organisationUUID = '{organisation_uuid}' 
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


def get_pool_details(cursor, organisation_uuid, user_uuid):
    try:
        logging.info("Getting pool details...")
        pools_sql = f"""
            SELECT DISTINCT a.poolUUID, a.pool_name, a.parentUUID
            FROM {database_dict['schema']}.{database_dict['pools_table']} a 
            JOIN {database_dict['schema']}.{database_dict['pools_users_table']} b on a.poolUUID = b.poolUUID AND b.userUUID = '{user_uuid}' and a.organisationUUID = '{organisation_uuid}'
        """
        cursor.execute(pools_sql)
        pools_result = cursor.fetchall()

        if pools_result:
            pool_ids = {pool[0] for pool in pools_result}
            processed_result = [(pool[0], pool[1], pool[2] if pool[2] in pool_ids else None,) for pool in
                                pools_result]
            pools_details = {pool[0]: {'Details': {'pool_name': pool[1], 'parentUUID': pool[2]}} for
                             pool in processed_result}
            return pools_details
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching pool details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_pool_users(cursor, organisation_details, pools_details):
    try:
        logging.info("Getting pool users...")
        if organisation_details and (organisation_details['permissionid'] <= 2) and pools_details:
            pools_users_sql = f"""
                SELECT DISTINCT b.poolUUID, a.userUUID, a.email
                FROM {database_dict['schema']}.{database_dict['users_table']} a
                JOIN {database_dict['schema']}.{database_dict['pools_users_table']} b
                ON a.userUUID = b.userUUID
                AND poolUUID IN ({','.join(repr(k) for k in pools_details.keys())})
                AND a.hub_user = 0
            """
            cursor.execute(pools_users_sql)
            pools_users_result = cursor.fetchall()
            if pools_users_result:
                pools_users = {}
                for pool_user in pools_users_result:
                    pool_uuid = pool_user[0]
                    user_uuid = pool_user[1]
                    email = pool_user[2]
                    if pool_uuid not in pools_users:
                        pools_users[pool_uuid] = {'Users': {user_uuid: {'email': email}}}
                    else:
                        pools_users[pool_uuid]['Users'][user_uuid] = {'email': email}
                return pools_users
            else:
                return {}
        else:
            return {}
    except mysql.connector.Error as e:
        logging.error(f"MySQL error fetching pool users: {e}")
        traceback.print_exc()
        raise Exception(f"Error fetching pool users: {e}", e.errno) from e
    except Exception as e:
        logging.error(f"Error fetching pool users: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def merge_pools_users_devices(pool_details, pools_users, pools_devices):
    try:
        logging.info("Merging pools, users, and devices...")
        merged_data = {}
        for pool_uuid, details in pool_details.items():
            merged_data[pool_uuid] = details.copy()  # Copy pool details
            users = pools_users.get(pool_uuid, {}).get('Users', {})  # Get users for the pool
            devices = pools_devices.get(pool_uuid, {}).get('Devices', [])  # Get devices for the pool
            merged_data[pool_uuid]['Users'] = users
            merged_data[pool_uuid]['Devices'] = devices
        return merged_data
    except Exception as e:
        logging.error(f"Error merging pools, users, and devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_device_details(cursor, org_uuid, organisation_details, user_uuid):
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
                device_details = {device[0]: {
                    'Details': {'long_address': device[1], 'short_address': device[2],
                                'device_name': device[3], 'registrant': device[4], 'device_type_id': device[5],
                                'associated_hub': device[6]}} for device in devices_details_result}
                return device_details
            else:
                return {}
        else:

            devices_details_sql = f"""
               SELECT DISTINCT a.deviceUUID, a.long_address, a.short_address,  a.device_name, a.registrant, a.device_type_id, a.associated_hub
                FROM {database_dict['schema']}.{database_dict['devices_table']} a 
                JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} b on a.deviceUUID = b.deviceUUID AND a.organisationUUID = '{org_uuid}'
                JOIN {database_dict['schema']}.{database_dict['pools_users_table']} c  on b.poolUUID = c.poolUUID and c.userUUID = '{user_uuid}'
                JOIN {database_dict['schema']}.{database_dict['pools_table']} d  on c.poolUUID = d.poolUUID and d.parentUUID IS NOT NULL

            """
            cursor.execute(devices_details_sql)
            devices_details_result = cursor.fetchall()

            if devices_details_result:
                device_details = {device[0]: {
                    'Details': {'long_address': device[1], 'short_address': device[2],
                                'device_name': device[3], 'registrant': device[4], 'device_type_id': device[5],
                                'associated_hub': device[6]}} for device in devices_details_result}
                return device_details
            else:
                return {}
    except Exception as e:
        logging.error(f"Error fetching device details: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_pools_devices(cursor, device_details):
    try:
        logging.info("Getting pools and associated devices...")
        if device_details:
            devices_pools_sql = f"""
                SELECT DISTINCT a.deviceUUID, a.poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_devices_table']} a
                WHERE deviceUUID IN ({','.join(repr(device_uuid) for device_uuid in device_details.keys())})

            """
            cursor.execute(devices_pools_sql)
            devices_pools_result = cursor.fetchall()

            pool_devices_dict = {}
            for device_uuid, pool_uuid in devices_pools_result:
                if pool_uuid in pool_devices_dict:
                    pool_devices_dict[pool_uuid].append(device_uuid)
                else:
                    pool_devices_dict[pool_uuid] = [device_uuid]

            pools_devices = {pool_uuid: {'Devices': device_uuids} for pool_uuid, device_uuids in pool_devices_dict.items()}
            return pools_devices
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching pools and associated devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def merge_pools_and_devices(pools_devices, pool_details):
    try:
        logging.info("Merging pools and devices...")
        pools = {}
        for device_uuid, pool_info in pools_devices.items():
            pool_uuid = pool_info.get('Pool')
            if pool_uuid:
                if pool_uuid not in pools:
                    pools[pool_uuid] = {'Details': pool_details.get(pool_uuid, {}), 'Devices': []}
                pools[pool_uuid]['Devices'].append(device_uuid)
        return pools
    except Exception as e:
        logging.error(f"Error merging pools and devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_hub_details(cursor, org_uuid, organisation_details):
    try:
        logging.info("Getting hub details...")
        if organisation_details:
            hub_details_sql = f"""
                SELECT DISTINCT a.hubUUID, a.serial, a.hub_name, a.registrant, a.device_type_id
                FROM {database_dict['schema']}.{database_dict['hubs_table']} a 
                WHERE organisationUUID = '{org_uuid}'
            """
            cursor.execute(hub_details_sql)
            hub_details_result = cursor.fetchall()

            if hub_details_result:
                hub_details = {hub[0]: {
                    'Details': {'serial': hub[1], 'hub_name': hub[2], 'registrant': hub[3],
                                'device_type_id': hub[4]}} for hub in hub_details_result}
                return hub_details
            else:
                return {}
        else:
            return {}
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
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor,
                                                                                           database_dict['schema'],
                                                                                           database_dict['users_table'],
                                                                                           user_email)
            organisation_details = get_organisation_details(cursor, user_uuid)
            if organisation_details:
                organisation_uuid = organisation_details['organisationUUID']
                organisation_users = get_organisation_users(cursor, organisation_uuid, organisation_details)
                organisation_invite_code = get_organisation_invite_code(cursor, organisation_uuid, organisation_details)
                device_details = get_device_details(cursor, organisation_uuid, organisation_details, user_uuid)
                pools_details = get_pool_details(cursor, organisation_uuid, user_uuid)
                pools_users = get_pool_users(cursor, organisation_details, pools_details)
                pools_devices = get_pools_devices(cursor, device_details)
                pools_merged = merge_pools_users_devices(pools_details, pools_users, pools_devices)
                hub_details = get_hub_details(cursor, organisation_uuid, organisation_details)

                output_dict = {
                    "organisationInfo": organisation_details,
                    "organisationUsers": organisation_users,
                    "organisationInviteCode": organisation_invite_code,
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
            body_value = 'Unable to retrive organisation details'
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




