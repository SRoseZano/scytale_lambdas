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

database_dict['schema'] = "zanocontrols"
database_dict['users_organisations_table'] = "users_organisations"
database_dict['pools_users_table'] = "pools_users"
database_dict['users_table'] = "users"
database_dict['pools_table'] = "pools"

zanolambdashelper.helpers.set_logging('INFO')


def has_permissions_to_remove_target(cursor, user_uuid, target_user_uuid, org_uuid):
    try:

        logging.info("Checking login user permissions...")

        sql = f"""
            SELECT DISTINCT permissionid
            FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
            WHERE a.userUUID = %s
            AND a.organisationUUID = %s
            LIMIT 1
        """

        cursor.execute(sql, (user_uuid, org_uuid))
        login_user_permissions = cursor.fetchone()

        logging.info("Checking target user permissions...")

        sql = f"""
            SELECT DISTINCT permissionid
            FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
            WHERE a.userUUID = %s
            AND a.organisationUUID = %s
            LIMIT 1
        """

        cursor.execute(sql, (target_user_uuid, org_uuid))
        target_user_permissions = cursor.fetchone()

    except Exception as e:
        logging.error(f"Error checking user permissions: {e}")
        traceback.print_exc()
        raise Exception(400, e)

    if target_user_permissions[0] < 3 or login_user_permissions[0] > 2:
        raise Exception(402, "Insufficient permissions to remove user from group")

def remove_user_from_pool(cursor, pool_uuid, target_user_uuid, org_uuid, user_uuid):
    try:

        get_entry = f"""
                             SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']}
                             WHERE userid = %s;
             """
        cursor.execute(get_entry, (target_user_uuid,))
        last_inserted_row = cursor.fetchall()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        logging.info("Executing SQL query to append device to pool:")
        logging.info(pool_uuid)
        # SQL query to add device to pool and all its children NOT NULL check to exclude trying to add NULL parent to table
        sql = f"""
            DELETE FROM {database_dict['schema']}.{database_dict['pools_users_table']}
                WHERE poolUUID IN (
                    WITH RECURSIVE PoolHierarchy AS (
                        SELECT parentUUID, poolUUID
                        FROM {database_dict['schema']}.{database_dict['pools_table']}
                        WHERE poolUUID = %s
                        UNION
                        SELECT p.parentUUID, p.poolUUID
                        FROM {database_dict['schema']}.{database_dict['pools_table']} p
                        JOIN PoolHierarchy ph ON p.parentUUID = ph.poolUUID
                    )
                    SELECT poolUUID FROM PoolHierarchy
                ) AND userUUID = %s;
            """

        cursor.execute(sql, (pool_uuid, target_user_uuid))
        sql_audit = sql % (pool_uuid, target_user_uuid)

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['pools_users_table'], 2, target_user_uuid, sql_audit,
            historic_row_json, '{}', org_uuid, user_uuid)

    except Exception as e:
        logging.error(f"Error removing user from pool: {e}")
        traceback.print_exc()
        raise Exception(400, e) from e


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

            has_permissions_to_remove_target(cursor, user_uuid, target_user_uuid, org_uuid)
            remove_user_from_pool(cursor, pool_uuid, target_user_uuid, org_uuid, user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422 or status_value == 402:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to remove user from pool'
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
        'body': 'User Removed To Pool Successfully'
    }
