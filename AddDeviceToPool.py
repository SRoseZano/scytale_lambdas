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

def get_current_device_pools(cursor, device_id):
    try:
        logging.info("Executing SQL query to get all pools currently belonging to device...")
        sql = f"""
            SELECT distinct p.poolID
            FROM pools_devices p
            WHERE deviceid = %s
            """
        cursor.execute(sql, (device_id,))
        sql_result = cursor.fetchall()
         # If the result is empty, return an empty list
        if sql_result:
            device_pools = [t[0] for t in sql_result]
        else:
            device_pools = []  # No pools found
            
        return device_pools;
    except Exception as e:
        logging.error(f"Error obtaining current device pools: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def get_potential_device_pools(cursor,pool_id, device_id):
    try:
        logging.info("Executing SQL query to get all pools that will belong to device...")
        sql = f"""
            WITH RECURSIVE PoolHierarchy AS (
                SELECT parentid, poolID
                FROM pools
                WHERE poolID = %s
    
                UNION
    
                SELECT p.parentid, p.poolID
                FROM pools p
                JOIN PoolHierarchy ph ON p.poolID = ph.parentID
            
            )
            SELECT %s AS deviceid, poolID
            FROM PoolHierarchy;
            """
        cursor.execute(sql, (pool_id,device_id,))
        
        sql_result = cursor.fetchall()
         # If the result is empty, return an empty list
        if sql_result:
            potential_device_pools = [t[1] for t in sql_result]
        else:
            potential_device_pools = []  # No pools found
            
        return potential_device_pools
    except Exception as e:
        logging.error(f"Error obtaining potential device pools: {e}")
        traceback.print_exc()
        raise Exception(400, e)
        

def append_device_to_pool(cursor, pool_id, device_id, org_UUID, user_UUID):
    try:
        logging.info("Executing SQL query to append device to pool...")

        # SQL query to add device to pool and its children
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_devices_table']} (deviceid, poolid)
            WITH RECURSIVE PoolHierarchy AS (
                SELECT parentid, poolID
                FROM {database_dict['schema']}.{database_dict['pools_table']}
                WHERE poolID = %s

                UNION

                SELECT p.parentid, p.poolID
                FROM {database_dict['schema']}.{database_dict['pools_table']} p
                JOIN PoolHierarchy ph ON p.poolID = ph.parentID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {database_dict['schema']}.{database_dict['pools_devices_table']} dp
                    WHERE dp.deviceid = %s
                    AND dp.poolid = p.poolID
                )
            )
            SELECT %s AS deviceid, poolID
            FROM PoolHierarchy;
        """

        # Attempt to execute the SQL query
        try:
            cursor.execute(sql, (pool_id, device_id, device_id))
            logging.info("SQL query executed successfully.")
        except Exception as e:
            logging.error(f"Error executing SQL query: {e}")
            traceback.print_exc()
            raise  # Re-raise to let the outer block handle it

        # Retrieve the last inserted row
        try:
            pool_id = zanolambdashelper.helpers.get_last_inserted_row()
            if pool_id is None:
                logging.error("Unable to get inserted row for audit logs.")
                raise ValueError("No row ID returned for inserted data.")
        except Exception as e:
            logging.error(f"Error retrieving last inserted row: {e}")
            traceback.print_exc()
            raise  # Re-raise to let the outer block handle it

        # Fetch and log the inserted row
        try:
            get_inserted_row_sql = f"""SELECT * FROM {database_dict['schema']}.{database_dict['pools_devices_table']} 
                                       WHERE poolid = %s and deviceid = %s LIMIT 1"""
            cursor.execute(get_inserted_row_sql, (pool_id,device_id))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                # Attempt to write to the audit log
                try:
                    zanolambdashelper.helpers.submit_to_audit_log(
                        cursor, database_dict['schema'], database_dict['audit_log_table'],
                        database_dict['pools_devices_table'], 3, pool_id, sql,
                        '{}', inserted_row_json, org_UUID, user_UUID
                    )
                    logging.info("Audit log submitted successfully.")
                except Exception as e:
                    logging.error(f"Error producing audit log: {e}")
                    traceback.print_exc()
                    raise  # Re-raise to let the outer block handle it
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found.")
        except Exception as e:
            logging.error(f"Error creating default pool entry inserted row: {e}")
            traceback.print_exc()
            raise  # Re-raise to let the outer block handle it

    except Exception as e:
        # Outermost block to capture and handle all exceptions
        logging.error(f"Unexpected error in append_device_to_pool: {e}")
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

        # Extract relevant attributes
        device_id_raw = body_json.get('device_id')
        pool_id_raw = body_json.get('pool_id')
        
        variables = {
            'device_id': {'value': device_id_raw['value'], 'value_type': device_id_raw['value_type']},
            'pool_id': {'value': pool_id_raw['value'], 'value_type': pool_id_raw['value_type']}
        }
                
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        device_id = variables['device_id']['value']
        pool_id = variables['pool_id']['value']
        

        with conn.cursor() as cursor:
            login_user_id, user_uuid = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id, org_uuid = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            zanolambdashelper.helpers.is_target_device_in_org(cursor,database_dict['schema'], database_dict['devices_table'], organisation_id, device_id)
            zanolambdashelper.helpers.is_target_pool_in_org(cursor,database_dict['schema'], database_dict['pools_table'], organisation_id, pool_id)
            current_device_pools = get_current_device_pools(cursor,device_id)
            potential_device_pools = get_potential_device_pools(cursor,pool_id,device_id)
            
            if (all(elem in potential_device_pools for elem in current_device_pools)): #check all pools in potential branch are in current branch (ensure device isnt in multiple branches)
                append_device_to_pool(cursor, pool_id, device_id, org_uuid, user_uuid)
            else:
                print("ERROR: New pool would be in different pool branch than current")
                raise Exception(401, "Error: New pool would be in different pool branch than current")
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation error
            body_value = e.args[1]
        elif status_value == 401: # if 401 then tree error
            body_value = e.args[1]
        else:
            body_value = 'Unable to add device to pool'
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
