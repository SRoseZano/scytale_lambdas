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

driver_devices = [2, 5]
controller_devices = [3, 4]


def get_organisation_details(cursor, user_uuid):
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


def get_organisation_users(cursor, org_uuid, organisation_details):
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


def get_organisation_invite_code(cursor, org_uuid, organisation_details):
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


def merge_pools_users_devices(pool_details, pools_devices):
    logging.info("Merging pools, users, and devices...")

    merged_data = {}
    for pool_id, details in pool_details.items():
        pool_uuid = details['Details']['poolUUID']
        merged_data[pool_id] = details.copy()  # Copy pool details
        devices = pools_devices.get(pool_uuid, {}).get('Devices', [])  # List of device IDs
        merged_data[pool_id]['Devices'] = devices

    return merged_data


def get_pool_details(cursor, org_uuid, device_uuid_to_id):
    if not device_uuid_to_id:
        logging.info("No device UUIDs provided — returning empty pool details.")
        return {}
    logging.info("Getting list of deviceUUIDS...")
    light_placeholders = ', '.join(['%s'] * len(driver_devices))
    controller_placeholders = ', '.join(['%s'] * len(controller_devices))

    logging.info("Getting list of deviceUUIDs...")
    device_uuids = list(device_uuid_to_id.keys())
    device_placeholders = ', '.join(['%s'] * len(device_uuids))

    logging.info("Getting pool details...")
    # Find all groups with controllers
    # join table on itsef to get rows that dont have their poolUUID as parentUUIDs of other rows (find lowest child)
    # get all groups with lighting devices
    # union all lowest level groups with controllers
    pools_sql = f"""
        WITH controllers_groups AS ( 
            SELECT DISTINCT a.poolUUID,  a.parentUUID 
            FROM {database_dict['schema']}.{database_dict['pools_table']} a
            JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} b ON a.poolUUID = b.poolUUID
            JOIN {database_dict['schema']}.{database_dict['devices_table']} d ON b.deviceUUID = d.deviceUUID
            WHERE d.device_type_id IN ({controller_placeholders}) AND a.organisationUUID = %s
        )

        SELECT DISTINCT a.poolUUID, a.parentUUID 
        FROM {database_dict['schema']}.{database_dict['pools_table']} a
        JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} b ON a.poolUUID = b.poolUUID
        JOIN {database_dict['schema']}.{database_dict['devices_table']} d ON b.deviceUUID = d.deviceUUID
        WHERE d.device_type_id IN ({light_placeholders}) AND a.organisationUUID = %s AND d.deviceUUID IN ({device_placeholders})

        UNION

        SELECT poolUUID,  parentUUID 
        FROM controllers_groups
    ;
    """

    cursor.execute(pools_sql, (controller_devices + [org_uuid] + driver_devices + [org_uuid] + device_uuids))
    pools_result = cursor.fetchall()
    print(pools_result)
    if pools_result:
        pool_uuids = {pool[0] for pool in pools_result}

        # Create a mapping of UUIDs to sequential integers
        uuid_to_int = {uuid: idx + 1 for idx, uuid in enumerate(pool_uuids)}

        processed_result = [(uuid_to_int[pool[0]], pool[0], uuid_to_int.get(pool[1], None)) for pool in
                            pools_result]
        pools_details = {pool[0]: {'Details': {'poolUUID': pool[1], 'parentUUID': pool[2]}}
                         for pool in processed_result}
        print(pools_details)
        return pools_details
    else:
        return {}


def get_device_details(cursor, org_uuid, organisation_details, hub_uuid_to_id, hub_uuid):
    logging.info("Getting device details...")

    if organisation_details and (organisation_details['permissionid'] <= 2):

        light_placeholders = ', '.join(['%s'] * len(driver_devices))
        controller_placeholders = ', '.join(['%s'] * len(controller_devices))

        devices_details_sql = f"""
            WITH lighting_devices AS (
                SELECT DISTINCT a.deviceUUID, a.long_address, a.short_address, a.device_type_id, a.associated_hub
                FROM {database_dict['schema']}.{database_dict['devices_table']} a 
                WHERE organisationUUID = %s AND associated_hub = %s AND device_type_id IN ({light_placeholders})
            ),

            controller_devices AS (
                SELECT DISTINCT d.deviceUUID, d.long_address, d.short_address, d.device_type_id, d.associated_hub
                FROM lighting_devices a 
                INNER JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} b on a.deviceUUID = b.deviceUUID
                INNER JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} c on b.poolUUID = c.poolUUID
                INNER JOIN {database_dict['schema']}.{database_dict['devices_table']} d on c.deviceUUID = d.deviceUUID
                WHERE d.organisationUUID = %s AND d.device_type_id IN ({controller_placeholders}) 
            ),

            controller_hub_associated AS (
                SELECT DISTINCT a.deviceUUID, a.long_address, a.short_address, a.device_type_id, a.associated_hub
                FROM {database_dict['schema']}.{database_dict['devices_table']} a 
                WHERE organisationUUID = %s AND associated_hub = %s AND device_type_id IN ({controller_placeholders})
            )


            SELECT * FROM lighting_devices UNION SELECT * FROM controller_devices UNION SELECT * FROM controller_hub_associated
        """
        cursor.execute(devices_details_sql, (
                [org_uuid] + [hub_uuid] + driver_devices + [org_uuid] + controller_devices + [org_uuid] + [
            hub_uuid] + controller_devices))
        devices_details_result = cursor.fetchall()
        print(devices_details_result)
        if devices_details_result:
            # Generate integer-based IDs for devices
            device_uuid_to_id = {device[0]: idx + 1 for idx, device in enumerate(devices_details_result)}
            devices_dict = {
                device_uuid_to_id[device[0]]: {
                    "Details": {
                        "deviceUUID": device[0],
                        "long_address": device[1],
                        "short_address": device[2],
                        "device_type_id": device[3],
                        "associated_hub": hub_uuid_to_id.get(device[4])  # Convert UUID to integer ID
                    }
                }
                for device in devices_details_result
            }
            return devices_dict, device_uuid_to_id  # Return dictionary & UUID-to-ID mapping
        else:
            return {}, {}


def get_pools_devices(cursor, device_uuid_to_id, org_uuid):
    logging.info("Getting pools and associated devices...")

    if not device_uuid_to_id:
        return {}

    device_uuids = list(device_uuid_to_id.keys())
    placeholders = ",".join(["%s"] * len(device_uuids))

    light_placeholders = ', '.join(['%s'] * len(driver_devices))
    controller_placeholders = ', '.join(['%s'] * len(controller_devices))

    logging.info("Getting list of deviceUUIDs...")
    device_uuids = list(device_uuid_to_id.keys())
    device_placeholders = ', '.join(['%s'] * len(device_uuids))

    # similar logic to getting all relevant groups
    # Find all groups/device pairs with controllers
    # join table on itsef to get rows that dont have their poolUUID as parentUUIDs of other rows (find lowest child)
    # get all groups/pairs with lighting devices
    # union in all lowest level groups with controllers

    devices_pools_sql = f"""

        WITH controllers_groups AS ( 
            SELECT DISTINCT a.poolUUID, d.deviceUUID, a.parentUUID
            FROM {database_dict['schema']}.{database_dict['pools_table']} a
            JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} b ON a.poolUUID = b.poolUUID
            JOIN {database_dict['schema']}.{database_dict['devices_table']} d ON b.deviceUUID = d.deviceUUID 
            WHERE d.device_type_id IN ({controller_placeholders}) AND a.organisationUUID = %s
        ),


        lowest_child_controllers AS ( 
            SELECT rc.poolUUID, rc.deviceUUID
            FROM controllers_groups rc
            LEFT JOIN controllers_groups ra ON rc.poolUUID = ra.parentUUID
            WHERE ra.parentUUID IS NULL 
        )

        SELECT DISTINCT d.deviceUUID, b.poolUUID
        FROM {database_dict['schema']}.{database_dict['pools_devices_table']} b
        JOIN {database_dict['schema']}.{database_dict['devices_table']} d ON b.deviceUUID = d.deviceUUID
        WHERE d.device_type_id IN ({light_placeholders}) AND d.organisationUUID = %s AND d.deviceUUID IN ({device_placeholders})

        UNION

        SELECT deviceUUID, poolUUID
        FROM lowest_child_controllers

    """
    cursor.execute(devices_pools_sql,
                   (controller_devices + [org_uuid] + driver_devices + [org_uuid] + device_uuids))
    devices_pools_result = cursor.fetchall()
    print(devices_pools_result)

    pool_devices_dict = {}
    for device_uuid, pool_uuid in devices_pools_result:
        device_id = device_uuid_to_id.get(device_uuid)  # Convert UUID to integer ID
        if device_id:
            if pool_uuid not in pool_devices_dict:
                pool_devices_dict[pool_uuid] = {'Devices': [device_id]}
            else:
                pool_devices_dict[pool_uuid]['Devices'].append(device_id)

    return pool_devices_dict


def get_hub_details(cursor, org_uuid, organisation_details, hub_uuid):
    logging.info("Getting hub details...")

    if not organisation_details:
        return {}, {}

    hub_details_sql = f"""
        SELECT DISTINCT a.hubUUID, a.serial, a.device_type_id,
                        b.long_address, b.short_address
        FROM {database_dict['schema']}.{database_dict['hubs_table']} a
        LEFT JOIN {database_dict['schema']}.{database_dict['hub_radios_table']} b
        ON a.hubUUID = b.hubUUID 
        WHERE a.hubUUID = %s AND a.organisationUUID = %s 
    """
    cursor.execute(hub_details_sql, (hub_uuid, org_uuid,))
    hub_details_result = cursor.fetchall()

    if not hub_details_result:
        return {}, {}

    # Group radios by hubUUID
    hub_map = {}
    for row in hub_details_result:
        hub_uuid, serial, device_type_id, long_addr, short_addr = row

        if hub_uuid not in hub_map:
            hub_map[hub_uuid] = {
                'hubUUID': hub_uuid,
                'serial': serial,
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
        print(body_json)
        hub_uuid_raw = body_json.get('hub_uuid')

        variables = {
            'hub_uuid': {'value': hub_uuid_raw['value'], 'value_type': 'uuid'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        hub_uuid = variables['hub_uuid']['value']

        with conn.cursor() as cursor:
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'],
                                                                            database_dict['users_table'], user_email)
            organisation_details = get_organisation_details(cursor, user_uuid)
            if organisation_details:
                organisation_uuid = organisation_details['organisationUUID']
                hub_details, hub_uuid_to_id = get_hub_details(cursor, organisation_uuid, organisation_details, hub_uuid)

                device_details, device_uuid_to_id = get_device_details(cursor, organisation_uuid,
                                                                       organisation_details, hub_uuid_to_id, hub_uuid)

                pools_details = get_pool_details(cursor, organisation_uuid,
                                                 device_uuid_to_id)  # parse the device mapping to get related groups

                pools_devices = get_pools_devices(cursor, device_uuid_to_id, organisation_uuid)
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
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to retrieve organisation details'
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

    if not output_dict:
        return {'statusCode': 204}
    else:
        return {'statusCode': 200, 'body': output_dict}




