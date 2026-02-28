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

# input_device_types = zanolambdashelper.helpers.get_input_device_types
input_device_types = [3, 4]


def is_input_device(cursor, device_uuid):
    logging.info("Executing SQL query to check for input device...")
    sql = f"""
        SELECT d.device_type_id
        FROM {database_dict['schema']}.{database_dict['devices_table']} d
        WHERE deviceUUID = %s
        LIMIT 1
    """
    cursor.execute(sql, (device_uuid,))
    result = cursor.fetchone()

    if result is None:
        raise Exception(f"No device found for UUID: {device_uuid}")

    device_type_id = result[0]
    is_input_device = device_type_id in input_device_types

    return is_input_device


def get_current_device_pools(cursor, device_uuid):
    logging.info("Executing SQL query to get all pools currently belonging to device...")
    sql = f"""
        SELECT distinct p.poolUUID
        FROM pools_devices p
        WHERE deviceUUID = %s
        """
    cursor.execute(sql, (device_uuid,))
    sql_result = cursor.fetchall()
    # If the result is empty, return an empty list
    if sql_result:
        device_pools = [t[0] for t in sql_result]
    else:
        device_pools = []  # No pools found

    return device_pools;


def get_current_input_device_pools(cursor, device_uuid):
    logging.info("Executing SQL query to get all pools currently belonging to device...")
    sql = f"""
        SELECT DISTINCT pd.poolUUID
        FROM {database_dict['schema']}.{database_dict['pools_devices_table']} AS pd
        INNER JOIN {database_dict['schema']}.{database_dict['pools_table']} AS p
            ON pd.poolUUID = p.poolUUID
        WHERE pd.deviceUUID = %s
        AND p.parentUUID IS NOT NULL;

        """
    cursor.execute(sql, (device_uuid,))
    sql_result = cursor.fetchall()
    # If the result is empty, return an empty list
    if sql_result:
        device_pools = [t[0] for t in sql_result]
    else:
        device_pools = []  # No pools found

    return device_pools;


def get_potential_device_pools(cursor, pool_uuid, device_uuid):
    logging.info("Executing SQL query to get all pools that will belong to device...")
    sql = f"""
        WITH RECURSIVE PoolHierarchy AS (
            SELECT parentUUID, poolUUID
            FROM pools
            WHERE poolUUID = %s

            UNION

            SELECT p.parentUUID, p.poolUUID
            FROM pools p
            JOIN PoolHierarchy ph ON p.poolUUID = ph.parentUUID

        )
        SELECT %s AS deviceUUID, poolUUID
        FROM PoolHierarchy;
        """
    cursor.execute(sql, (pool_uuid, device_uuid,))

    sql_result = cursor.fetchall()
    # If the result is empty, return an empty list
    if sql_result:
        potential_device_pools = [t[1] for t in sql_result]
    else:
        potential_device_pools = []  # No pools found

    return potential_device_pools


def append_input_device_to_pool(cursor, pool_uuid, device_uuid, org_uuid, user_uuid):
    logging.info("Executing SQL query to append device to pool non recursively...")
    # SQL query to add device to pool
    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['pools_devices_table']} (deviceUUID, poolUUID) VALUES (%s, %s)
    """
    cursor.execute(sql, (device_uuid, pool_uuid))


def append_device_to_pool(cursor, pool_uuid, device_uuid, org_uuid, user_uuid):
    logging.info("Executing SQL query to append device to pool...")
    # SQL query to add device to pool and its children
    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['pools_devices_table']} (deviceUUID, poolUUID)
        WITH RECURSIVE PoolHierarchy AS (
            SELECT parentUUID, poolUUID
            FROM {database_dict['schema']}.{database_dict['pools_table']}
            WHERE poolUUID = %s

            UNION

            SELECT p.parentUUID, p.poolUUID
            FROM {database_dict['schema']}.{database_dict['pools_table']} p
            JOIN PoolHierarchy ph ON p.poolUUID = ph.parentUUID
            WHERE NOT EXISTS (
                SELECT 1
                FROM {database_dict['schema']}.{database_dict['pools_devices_table']} dp
                WHERE dp.deviceUUID = %s
                AND dp.poolUUID = p.poolUUID
            )
        )
        SELECT %s AS deviceUUID, poolUUID
        FROM PoolHierarchy;
    """

    cursor.execute(sql, (pool_uuid, device_uuid, device_uuid,))


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
        device_uuid_raw = body_json.get('device_uuid')
        pool_uuid_raw = body_json.get('pool_uuid')

        variables = {
            'device_uuid': {'value': device_uuid_raw['value'], 'value_type': 'uuid'},
            'pool_uuid': {'value': pool_uuid_raw['value'], 'value_type': 'uuid'}
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        device_uuid = variables['device_uuid']['value']
        pool_uuid = variables['pool_uuid']['value']

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
            zanolambdashelper.helpers.is_target_device_in_org(cursor, database_dict['schema'],
                                                              database_dict['devices_table'], org_uuid,
                                                              device_uuid)
            zanolambdashelper.helpers.is_target_pool_in_org(cursor, database_dict['schema'],
                                                            database_dict['pools_table'], org_uuid, pool_uuid)
            if is_input_device(cursor, device_uuid):
                current_device_pools = get_current_input_device_pools(cursor, device_uuid)
                if not current_device_pools:
                    append_input_device_to_pool(cursor, pool_uuid, device_uuid, org_uuid, user_uuid)
                else:
                    raise Exception(401,
                                    "Error: This device can only belong to one group, please remove from existing group and try again")

            else:
                current_device_pools = get_current_device_pools(cursor, device_uuid)
                potential_device_pools = get_potential_device_pools(cursor, pool_uuid, device_uuid)

                if (all(elem in potential_device_pools for elem in
                        current_device_pools)):  # check all pools in potential branch are in current branch (ensure device isnt in multiple branches)
                    append_device_to_pool(cursor, pool_uuid, device_uuid, org_uuid, user_uuid)
                else:
                    raise Exception(401, "Error: New pool would be in different pool branch than current")
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to add device to pool'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 401:  # if 422 then validation error
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
        'body': 'Device Added To Pool Successfully'
    }
