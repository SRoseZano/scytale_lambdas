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

rds_client =  zanolambdashelper.helpers.create_client('rds') 

zanolambdashelper.helpers.set_logging('INFO')

max_org_devices = 500


def get_org_device_count(cursor, organisation_id):
    try:
        logging.info("Fetching org device count...")
        sql = f"SELECT COUNT(DISTINCT deviceid) FROM {database_dict['schema']}.{database_dict['devices_table']} WHERE organisationid = %s"
        cursor.execute(sql, (organisation_id,))
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error fetching org device count: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def get_default_pool_id(cursor, organisation_id):
    try:
        logging.info("Fetching default pool ID...")
        sql = f"SELECT poolid FROM {database_dict['schema']}.{database_dict['pools_table']} WHERE organisationid = %s and parentid is null"
        cursor.execute(sql, (organisation_id,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError("Unable to gather default pool")
    except Exception as e:
        logging.error(f"Error fetching default pool ID: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def create_device(cursor, long_address, short_address, device_type_id, associated_hub,  user_email, device_name, organisation_id):
    try:
        logging.info("Creating device entry...")
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['devices_table']} (long_address, short_address, device_type_id, associated_hub, registrant, device_name, organisationid) \
                VALUES (%s, %s, %s, %s,%s, %s, %s)"
        cursor.execute(sql, (long_address, short_address, device_type_id, associated_hub,  user_email, device_name, organisation_id))
        
        # Fetch the ID of the newly inserted device
        sql_fetch_last_id = "SELECT LAST_INSERT_ID();"
        cursor.execute(sql_fetch_last_id)
        device_id = cursor.fetchone()[0]
        
        return device_id
    except Exception as e:
        logging.error(f"Error creating device entry: {e}")
        traceback.print_exc()
        raise Exception(400, e) from e


def add_device_to_default_pool(cursor, pool_id, device_id):
    try:
        logging.info("Adding device to default pool...")
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['pools_devices_table']} (poolid, deviceid) VALUES (%s, %s)"
        cursor.execute(sql, (pool_id, device_id))
    except Exception as e:
        logging.error(f"Error adding device to default pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)
        
def gather_device_uuid(cursor,device_id):
    try:
        logging.info("Gathering topic from device...")
        sql = f"SELECT deviceUUID FROM {database_dict['schema']}.{database_dict['devices_table']} WHERE deviceid = %s"
        cursor.execute(sql, (device_id,))
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error gathering topic from device: {e}")
        traceback.print_exc()
        raise Exception(400, e)




def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
        conn.autocommit = False 
    
        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)
    
        # Extract relevant attributes if non existant set empty
    
        device_name_raw = body_json.get('device_name')
        long_address_raw = body_json.get('long_address')
        short_address_raw = body_json.get('short_address')
        device_type_id_raw = body_json.get('device_type_id')
        associated_hub_raw =  body_json.get('associated_hub')
    
        variables = {
            'device_name': {'value': device_name_raw['value'], 'value_type': device_name_raw['value_type']},
            'long_address': {'value': long_address_raw['value'], 'value_type': long_address_raw['value_type']},
            'short_address': {'value': short_address_raw['value'], 'value_type': short_address_raw['value_type']},
            'device_type_id': {'value': device_type_id_raw['value'], 'value_type': device_type_id_raw['value_type']},
            'associated_hub': {'value': associated_hub_raw['value'], 'value_type': associated_hub_raw['value_type']},
        }
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        device_name = variables['device_name']['value']
        long_address = variables['long_address']['value']
        short_address = variables['short_address']['value']
        device_type_id = variables['device_type_id']['value']
        associated_hub = variables['associated_hub']['value']

        with conn.cursor() as cursor:

            login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            
            org_device_count = get_org_device_count(cursor,organisation_id)
            if org_device_count + 1 > max_org_devices: #if device count with new device is greater max then raise custom exception
                logging.error("Org is at device limit...")
                raise Exception(403, f"You have reached your organisations device limit of {max_org_devices}")
                
            default_pool_id = get_default_pool_id(cursor,organisation_id)
            device_id = create_device(cursor, long_address, short_address, device_type_id, associated_hub,  user_email, device_name, organisation_id)
            pool_id = get_default_pool_id(cursor, organisation_id)
            add_device_to_default_pool(cursor, pool_id, device_id)
            device_topic = gather_device_uuid(cursor, device_id)
            conn.commit()


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 403: # if 422 then validation error
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
        'device_topic': device_topic
    }
