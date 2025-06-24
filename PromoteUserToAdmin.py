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


def append_user_to_all_pools(cursor, org_uuid, user_uuid):
    try:
        logging.info("Executing SQL query to append user to all org pools...")
        # SQL query to find top level pool and assign to everyone under it
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userUUID, poolUUID)
            WITH RECURSIVE PoolHierarchy AS (
                SELECT parentUUID, poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_table']}
                WHERE parentUUID is NULL AND organisationUUID = %s

                UNION

                SELECT p.parentUUID, p.poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_table']} p
                JOIN PoolHierarchy ph ON ph.poolUUID = p.parentUUID

            )
            SELECT %s AS userUUID, poolUUID
            FROM PoolHierarchy ph
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM {database_dict['schema']}.{database_dict['pools_users_table']} dp
                    WHERE dp.userUUID = %s
                    AND dp.poolUUID = ph.poolUUID
                );

        """

        cursor.execute(sql, (org_uuid, user_uuid, user_uuid))

        sql_audit = sql % (org_uuid, user_uuid, user_uuid)

        get_current_entry = f"""
                                SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']}
                                WHERE userUUID = %s 
                            """
        cursor.execute(get_current_entry, (user_uuid,))
        last_inserted_row = cursor.fetchall()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['pools_users_table'], 3, user_uuid, sql_audit,
            '{}', current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def promote_user_to_admin(cursor, org_uuid, user_uuid):
    try:

        get_entry = f"""
                    SELECT * FROM {database_dict['schema']}.{database_dict['users_organisations_table']}
                    WHERE organisationUUID = %s AND userUUID = %s;

        """
        cursor.execute(get_entry, (org_uuid, user_uuid))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")
        logging.info("Executing SQL query promote user to admin")

        sql = f"""
            UPDATE {database_dict['schema']}.{database_dict['users_organisations_table']}
            SET permissionID = 2
            WHERE organisationUUID = %s AND userUUID = %s;

        """
        cursor.execute(sql, (org_uuid, user_uuid))

        sql_audit = sql % (org_uuid, user_uuid)

        cursor.execute(get_entry, (org_uuid, user_uuid))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['pools_table'], 1, org_uuid, sql_audit,
            historic_row_json, current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error promoting user to admin: {e}")
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

        variables = {
            'user_uuid': {'value': user_uuid_raw['value'], 'value_type': 'uuid'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        target_user_uuid = variables['user_uuid']['value']

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
            append_user_to_all_pools(cursor, org_uuid, target_user_uuid)
            promote_user_to_admin(cursor, org_uuid, target_user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to promote user'
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
        'body': 'User Promoted To Admin Successfully'
    }