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

rds_client =  zanolambdashelper.helpers.create_client('rds') 

print("gotten the client")


zanolambdashelper.helpers.set_logging('INFO')

def get_organisation_details(cursor, login_user_id):
    try:
        logging.info("Getting organisation details...")
        organisation_details_sql = f"""
            SELECT DISTINCT a.*, b.permissionid FROM {database_dict['schema']}.{database_dict['organisations_table']} a 
            JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b 
            ON a.organisationid = b.organisationid
            AND b.userid = {login_user_id}
            LIMIT 1
        """
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

def get_organisation_users(cursor, organisation_id, organisation_details):
    try:
        logging.info("Getting organisation users...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            organisation_users_sql = f"""
                SELECT DISTINCT a.userid, a.email, b.permissionid
                FROM {database_dict['schema']}.{database_dict['users_table']} a
                JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b 
                ON a.userid = b.userid 
                AND a.hub_user = 0
                AND b.organisationid = {organisation_id}
            """
            cursor.execute(organisation_users_sql)
            organisation_users_result = cursor.fetchall()
            # Convert the raw users output to a dictionary with userid as the key
            organisation_users = {user[0]: {'email': user[1], 'permissionid': user[2]} for user in organisation_users_result}
            return organisation_users
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching organisation users: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def get_organisation_invite_code(cursor, organisation_id, organisation_details):
    try:
        logging.info("Getting organisation invite codes...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            organisation_invite_code_sql = current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            organisation_invite_code_sql = f"""SELECT DISTINCT invite_code 
                                                FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} 
                                                WHERE organisationID = {organisation_id} 
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

def get_pool_details(cursor, organisation_id, login_user_id):
    try:
        logging.info("Getting pool details...")
        pools_sql = f"""
            SELECT DISTINCT a.poolid, a.poolUUID, a.pool_name, a.parentid
            FROM {database_dict['schema']}.{database_dict['pools_table']} a 
            JOIN {database_dict['schema']}.{database_dict['pools_users_table']} b on a.poolid = b.poolid AND b.userid = {login_user_id} and a.organisationid = {organisation_id}
        """
        cursor.execute(pools_sql)
        pools_result = cursor.fetchall()

        if pools_result:
            pool_ids = {pool[0] for pool in pools_result}
            processed_result =  [(pool[0], pool[1], pool[2], pool[3] if pool[3] in pool_ids else None, ) for pool in pools_result]
            pools_details = {pool[0]: { 'Details': {'poolUUID': pool[1],'pool_name': pool[2], 'parentid': pool[3]}} for pool in processed_result}
            return pools_details
        else:
            return {}
    except Exception as e:
        logging.error(f"Error fetching pool details: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def get_pool_users(cursor, organisation_id, organisation_details, pools_details):
    try:
        logging.info("Getting pool users...")
        if organisation_details and (organisation_details['permissionid'] <= 2) and pools_details:
            pools_users_sql = f"""
                SELECT DISTINCT b.poolid, a.userid, a.email
                FROM {database_dict['schema']}.{database_dict['users_table']} a
                JOIN {database_dict['schema']}.{database_dict['pools_users_table']} b
                ON a.userid = b.userid 
                AND poolid IN ({','.join(map(str, list(pools_details.keys())))})
                AND a.hub_user = 0
            """
            cursor.execute(pools_users_sql)
            pools_users_result = cursor.fetchall()
            if pools_users_result:
                pools_users = {}
                for pool_user in pools_users_result:
                    pool_id = pool_user[0]
                    user_id = pool_user[1]
                    email = pool_user[2]
                    if pool_id not in pools_users:
                        pools_users[pool_id] = {'Users': {user_id: {'email': email}}}
                    else:
                        pools_users[pool_id]['Users'][user_id] = {'email': email}
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
        for pool_id, details in pool_details.items():
            merged_data[pool_id] = details.copy()  # Copy pool details
            users = pools_users.get(pool_id, {}).get('Users', {})  # Get users for the pool
            devices = pools_devices.get(pool_id, {}).get('Devices', [])  # Get devices for the pool
            merged_data[pool_id]['Users'] = users
            merged_data[pool_id]['Devices'] = devices
        return merged_data
    except Exception as e:
        logging.error(f"Error merging pools, users, and devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_device_details(cursor, organisation_id, organisation_details, login_user_id):
    try:
        logging.info("Getting device details...")
        if organisation_details and (organisation_details['permissionid'] <= 2):
            devices_details_sql = f"""
                SELECT DISTINCT a.deviceID, a.deviceUUID, a.long_address, a.short_address,  a.device_name, a.registrant, a.device_type_id, a.associated_hub
                FROM {database_dict['schema']}.{database_dict['devices_table']} a 
                WHERE organisationid = {organisation_id}
            """
            cursor.execute(devices_details_sql)
            devices_details_result = cursor.fetchall()

            if devices_details_result:
                device_details = {device[0]: { 'Details': {'deviceUUID': device[1],'long_address': device[2],'short_address': device[3],'device_name': device[4], 'registrant': device[5], 'device_type_id': device[6], 'associated_hub': device[7]}} for device in devices_details_result}
                return device_details
            else:
                return {}
        else:
            print("HERE");
            devices_details_sql = f"""
               SELECT DISTINCT a.deviceID, a.deviceUUID, a.long_address, a.short_address,  a.device_name, a.registrant, a.device_type_id, a.associated_hub
                FROM {database_dict['schema']}.{database_dict['devices_table']} a 
                JOIN {database_dict['schema']}.{database_dict['pools_devices_table']} b on a.deviceID = b.deviceID AND a.organisationid = {organisation_id}
                JOIN {database_dict['schema']}.{database_dict['pools_users_table']} c  on b.poolid = c.poolid and c.userid = {login_user_id}
                JOIN {database_dict['schema']}.{database_dict['pools_table']} d  on c.poolid = d.poolid and d.parentid IS NOT NULL
                
            """
            cursor.execute(devices_details_sql)
            devices_details_result = cursor.fetchall()

            if devices_details_result:
                device_details = {device[0]: { 'Details': {'deviceUUID': device[1],'long_address': device[2],'short_address': device[3],'device_name': device[4], 'registrant': device[5], 'device_type_id': device[6], 'associated_hub': device[6]}} for device in devices_details_result}
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
                SELECT DISTINCT a.deviceID, a.poolid
                FROM {database_dict['schema']}.{database_dict['pools_devices_table']} a
                WHERE deviceid IN ({','.join([f"'{device_id}'" for device_id in device_details.keys()])})
            """
            cursor.execute(devices_pools_sql)
            devices_pools_result = cursor.fetchall()

            pool_devices_dict = {}
            for device_id, pool_id in devices_pools_result:
                if pool_id in pool_devices_dict:
                    pool_devices_dict[pool_id].append(device_id)
                else:
                    pool_devices_dict[pool_id] = [device_id]

            pools_devices = {pool_id: {'Devices': device_ids} for pool_id, device_ids in pool_devices_dict.items()}
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
        for device_id, pool_info in pools_devices.items():
            pool_id = pool_info.get('Pool')
            if pool_id:
                if pool_id not in pools:
                    pools[pool_id] = {'Details': pool_details.get(pool_id, {}), 'Devices': []}
                pools[pool_id]['Devices'].append(device_id)
        return pools
    except Exception as e:
        logging.error(f"Error merging pools and devices: {e}")
        traceback.print_exc()
        raise Exception(400, e)

        
def get_hub_details(cursor, organisation_id, organisation_details):
    try:
        logging.info("Getting hub details...")
        if organisation_details:
            hub_details_sql = f"""
                SELECT DISTINCT a.hubID, a.hubUUID, a.serial, a.hub_name, a.registrant, a.device_type_id
                FROM {database_dict['schema']}.{database_dict['hubs_table']} a 
                WHERE organisationid = {organisation_id}
            """
            cursor.execute(hub_details_sql)
            hub_details_result = cursor.fetchall()

            if hub_details_result:
                hub_details = {hub[0]: { 'Details': {'hubUUID': hub[1],'serial': hub[2],'hub_name': hub[3],'registrant': hub[4], 'device_type_id': hub[5]}} for hub in hub_details_result}
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
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)
        print(database_token)
        conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
        print("here")
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['params']['querystring']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        with conn.cursor() as cursor:
                login_user_id, user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
                organisation_details, org_uuid = get_organisation_details(cursor, login_user_id)
                if organisation_details:
                    organisation_id = organisation_details['organisationID']
                    organisation_users = get_organisation_users(cursor, organisation_id, organisation_details)
                    organisation_invite_code = get_organisation_invite_code(cursor,organisation_id, organisation_details)
                    device_details = get_device_details(cursor, organisation_id, organisation_details, login_user_id)
                    pools_details = get_pool_details(cursor, organisation_id, login_user_id)
                    pools_users = get_pool_users(cursor, organisation_id, organisation_details, pools_details)
                    pools_devices = get_pools_devices(cursor, device_details)
                    pools_merged = merge_pools_users_devices(pools_details, pools_users, pools_devices)
                    hub_details = get_hub_details(cursor, organisation_id, organisation_details)

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
        if status_value == 422: # if 422 then validation error
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


           

