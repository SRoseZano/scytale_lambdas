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

max_pool_count = 100


def count_pools(cursor, org_uuid):
    try:
        logging.info("Checking current org pool count...")
        sql = f"SELECT count(DISTINCT poolUUID) FROM {database_dict['schema']}.{database_dict['pools_table']} WHERE organisationUUID = %s"
        cursor.execute(sql, (org_uuid,))
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error getting pool count: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_pool(cursor, pool_name, parent_uuid, org_uuid, user_uuid):
    try:
        logging.info("Creating pool...")
        pool_uuid = zanolambdashelper.helpers.generate_time_based_uuid(user_uuid, pool_name)
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['pools_table']} (poolUUID,organisationUUID, pool_name, parentUUID) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (pool_uuid,org_uuid, pool_name, parent_uuid))
        sql_audit = sql % (pool_uuid,org_uuid, pool_name, parent_uuid)

        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_table']} 
                        WHERE poolUUID = %s  LIMIT 1
                    """
            cursor.execute(get_inserted_row_sql, (pool_uuid,))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['pools_table'], 3, pool_uuid, sql_audit,
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
        return pool_uuid
    except Exception as e:
        logging.error(f"Error inserting pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def inherit_parent_users_into_pool(cursor, pool_uuid, parent_uuid, org_uuid, user_uuid):
    try:
        logging.info("Inserting admin users of parent pool into new pool...")
        sql = f"""INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (poolUUID, userUUID) 
        SELECT %s, a.userUUID
        FROM {database_dict['schema']}.{database_dict['pools_users_table']} a
        JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} b
        ON a.userUUID = b.userUUID AND b.permissionid <= 2 AND a.poolUUID = %s

        UNION

        SELECT %s, a.userUUID
        FROM {database_dict['schema']}.{database_dict['pools_users_table']} a
        JOIN {database_dict['schema']}.{database_dict['pools_table']} b
        ON a.poolUUID = b.poolUUID AND b.parentUUID IS NOT NULL AND a.poolUUID = %s

        """
        cursor.execute(sql, (pool_uuid, parent_uuid, pool_uuid, parent_uuid))

        sql_audit = sql % (pool_uuid, parent_uuid, pool_uuid, parent_uuid)

        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']} 
                        WHERE poolUUID = %s AND userUUID = %s LIMIT 1
                    """
            cursor.execute(get_inserted_row_sql, (pool_uuid, user_uuid))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['pools_users_table'], 3, pool_uuid, sql_audit,
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


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        pool_name_raw = body_json.get('pool_name')
        parent_uuid_raw = body_json.get('parent_uuid')

        variables = {
            'pool_name': {'value': pool_name_raw['value'], 'value_type': 'string_input'},
            'parent_uuid': {'value': parent_uuid_raw['value'], 'value_type': 'uuid'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        pool_name = variables['pool_name']['value']
        parent_uuid = variables['parent_uuid']['value']

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
            zanolambdashelper.helpers.is_target_pool_in_org(cursor, database_dict['schema'],
                                                            database_dict['pools_table'], org_uuid, parent_uuid)

            pool_count = count_pools(cursor, org_uuid)
            if pool_count + 1 > max_pool_count:  # if pool count with new pool is greater max then raise custom exception
                logging.error("Org is at group limit...")
                raise Exception(403, f"You have reached your organisations group limit of {max_pool_count}")
            pool_uuid = create_pool(cursor, pool_name, parent_uuid, org_uuid, user_uuid)
            inherit_parent_users_into_pool(cursor, pool_uuid, parent_uuid, org_uuid, user_uuid)
            pool_topic = pool_uuid
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 403:  # if 422 then validation
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
