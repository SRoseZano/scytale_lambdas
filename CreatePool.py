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

max_pool_count = 100

def count_pools(cursor, organisation_id):
    try:
        logging.info("Checking current org pool count...")
        sql = f"SELECT count(DISTINCT poolid) FROM {database_dict['schema']}.{database_dict['pools_table']} WHERE organisationid = %s"
        cursor.execute(sql, (organisation_id,))
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error getting pool count: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def create_pool(cursor, organisation_id, pool_name, parent_id, org_uuid, user_uuid):
    try:
        logging.info("Creating pool...")
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['pools_table']} (organisationid, pool_name, parentid) VALUES (%s, %s, %s)"
        cursor.execute(sql, (organisation_id, pool_name, parent_id))

        # Fetch the last inserted ID
        try:
            pool_id = zanolambdashelper.helpers.get_last_inserted_row()
            if not pool_id:
                raise ValueError("No pool ID returned after insertion.")
            logging.info(f"Default pool created with ID: {pool_id}")
        except Exception as e:
            logging.error(f"Error fetching the last inserted pool ID: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_table']} 
                        WHERE poolid = %s  LIMIT 1
                    """
            cursor.execute(get_inserted_row_sql, (pool_id,))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['pools_table'], 3, pool_id, sql,
                    '{}', inserted_row_json, org_uuid, user_uuid
                )
                logging.info("Audit log submitted successfully.")
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error creating audit log: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block
    except Exception as e:
        logging.error(f"Error inserting pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)
        

def fetch_last_inserted_id(cursor):
    try:
        logging.info("Getting newly created poolid...")
        cursor.execute("SELECT LAST_INSERT_ID();")
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error fetching last inserted ID: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def inherit_parent_users_into_pool(cursor, pool_id, parent_id, login_user_id, org_uuid, user_uuid):
    try:
        logging.info("Inserting admin users of parent pool into new pool...")
        sql = f"""INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (poolid, userid) 
        SELECT %s, a.userid 
        FROM {database_dict['schema']}.{database_dict['pools_users_table']} a
        JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b
        ON a.userid = b.userid AND b.permissionid = 1 AND a.poolid = %s
        
        UNION
        
        SELECT %s, a.userid 
        FROM {database_dict['schema']}.{database_dict['pools_users_table']} a
        JOIN {database_dict['schema']}.{database_dict['pools_table']} b
        ON a.poolid = b.poolid AND b.parentid IS NOT NULL AND a.poolid = %s
        
        """
        cursor.execute(sql, (pool_id, parent_id, pool_id, parent_id))



        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']} 
                        WHERE poolid = %s AND userid = %s LIMIT 1
                    """
            cursor.execute(get_inserted_row_sql, (pool_id, login_user_id))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['pools_users_table'], 3, pool_id, sql,
                    '{}', inserted_row_json, org_uuid, user_uuid
                )
                logging.info("Audit log submitted successfully.")
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error creating audit log: {e}")
            traceback.print_exc()
            raise

    except Exception as e:
        logging.error(f"Error inserting users of parent pool into new pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)
        
def gather_pool_uuid(cursor,pool_id):
    try:
        logging.info("Gathering topic from pool...")
        sql = f"SELECT poolUUID FROM {database_dict['schema']}.{database_dict['pools_table']} WHERE poolid = %s"
        cursor.execute(sql, (pool_id,))
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error gathering topic from pool: {e}")
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


        pool_name_raw = body_json.get('pool_name')
        parent_id_raw = body_json.get('parent_id')
        

        variables = {
            'pool_name': {'value': pool_name_raw['value'], 'value_type': pool_name_raw['value_type']},
            'parent_id': {'value': parent_id_raw['value'], 'value_type': parent_id_raw['value_type']},
        }
        
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        pool_name = variables['pool_name']['value']
        parent_id = variables['parent_id']['value']

        with conn.cursor() as cursor:
            
            login_user_id, user_uuid = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id, org_uuid = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            print(organisation_id)
            
            print(database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            zanolambdashelper.helpers.is_target_pool_in_org(cursor,database_dict['schema'],database_dict['pools_table'], organisation_id, parent_id)
            
            pool_count = count_pools(cursor, organisation_id)
            if pool_count + 1 > max_pool_count: #if pool count with new pool is greater max then raise custom exception
                logging.error("Org is at group limit...")
                raise Exception(403, f"You have reached your organisations group limit of {max_pool_count}")
            create_pool(cursor, organisation_id, pool_name, parent_id, org_uuid, user_uuid)
            pool_id = fetch_last_inserted_id(cursor)
            inherit_parent_users_into_pool(cursor, pool_id, parent_id, login_user_id, org_uuid, user_uuid)
            pool_topic = gather_pool_uuid(cursor, pool_id)
            conn.commit()
            
    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 403: # if 422 then validation 
            body_value = e.args[1]
        else:
            body_value = 'Unable to create pool'
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
        'body': 'Pool Added Successfully',
        'pool_topic': pool_topic
    }
