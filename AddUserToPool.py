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


def append_user_to_pool(cursor, pool_uuid, target_user_uuid, org_uuid, user_uuid):
    try:
        logging.info(f"Executing SQL query to append user to pool:{pool_uuid}")

        # Step 1: Create default pool entry in database
        # SQL query to add device to pool and all its children NOT NULL check to exclude trying to add NULL parent to table
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userUUID, poolUUID)
            WITH RECURSIVE PoolHierarchy AS (
                SELECT parentUUID, poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_table']}
                WHERE poolUUID = %s

                UNION

                SELECT p.parentUUID, p.poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_table']} p
                JOIN PoolHierarchy ph ON ph.poolUUID = p.parentUUID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {database_dict['schema']}.{database_dict['pools_users_table']} dp
                    WHERE dp.userUUID = %s
                    AND dp.poolUUID = p.poolUUID
                )

            )
            SELECT %s AS userUUID, poolUUID
            FROM PoolHierarchy;

        """

        cursor.execute(sql, (pool_uuid, target_user_uuid, target_user_uuid))

        sql_audit = sql % (pool_uuid, target_user_uuid, target_user_uuid)

        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']} 
                        WHERE userUUID = %s LIMIT 1
                    """
            cursor.execute(get_inserted_row_sql, (target_user_uuid,))
            last_inserted_row = cursor.fetchall()

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
            raise  # Re-raise to propagate to the outer block

    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
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

        # Extract relevant attributes
        user_uuid_raw = body_json.get('user_uuid')
        pool_uuid_raw = body_json.get('pool_uuid')

        variables = {
            'user_uuid': {'value': user_uuid_raw['value'], 'value_type': 'uuid'},
            'pool_uuid': {'value': pool_uuid_raw['value'], 'value_type': 'uuid'}
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        target_user_uuid = variables['user_uuid']['value']
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
            zanolambdashelper.helpers.is_target_user_in_org(cursor, database_dict['schema'],
                                                            database_dict['users_organisations_table'], org_uuid,
                                                            target_user_uuid)
            zanolambdashelper.helpers.is_target_pool_in_org(cursor, database_dict['schema'],
                                                            database_dict['pools_table'], org_uuid, pool_uuid)

            append_user_to_pool(cursor, pool_uuid, target_user_uuid, org_uuid, user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to add user to pool'
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
        'body': 'User Added To Pool Successfully'
    }
