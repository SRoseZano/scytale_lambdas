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


def can_user_be_demoted(cursor, organisation_uuid, user_uuid, target_user_uuid):
    logging.info("Executing SQL query to check if user can be demoted")

    sql = f"""
        SELECT permissionid
        FROM {database_dict['schema']}.{database_dict['users_organisations_table']}
        WHERE organisationUUID = %s AND userUUID = %s;

    """

    cursor.execute(sql, (organisation_uuid, user_uuid))

    user_result, = cursor.fetchone()

    sql = f"""
        SELECT permissionid
        FROM {database_dict['schema']}.{database_dict['users_organisations_table']}
        WHERE organisationUUID = %s AND userUUID = %s;

    """

    cursor.execute(sql, (organisation_uuid, target_user_uuid))

    target_user_result, = cursor.fetchone()

    if user_result < target_user_result:  # if permissions of user is higher than target
        return True
    else:
        raise Exception(400, "You do not have permissions to demote target user")


def demote_user(cursor, org_uuid, user_uuid, target_user_uuid):
    logging.info("Executing SQL query to demote user to admin")

    sql = f"""
        UPDATE {database_dict['schema']}.{database_dict['users_organisations_table']}
        SET permissionID = 3
        WHERE organisationUUID = %s AND userUUID = %s;

    """

    cursor.execute(sql, (org_uuid, target_user_uuid))

    logging.info("Executing SQL query to remove user from any of the organisation pools...")

    sql = f"""
        DELETE p
        FROM {database_dict['schema']}.{database_dict['pools_users_table']} p 
        INNER JOIN {database_dict['schema']}.{database_dict['pools_table']} a ON p.poolUUID = a.poolUUID 
        WHERE p.userUUID = %s AND a.organisationUUID = %s AND a.parentUUID IS NOT NULL;
        """

    cursor.execute(sql, (target_user_uuid, org_uuid))


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
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'],
                                                                            database_dict['users_table'], user_email)
            org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor, database_dict['schema'],
                                                                               database_dict[
                                                                                   'users_organisations_table'],
                                                                               user_uuid)

            # validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], user_uuid, org_uuid)
            zanolambdashelper.helpers.is_target_user_in_org(cursor, database_dict['schema'],
                                                            database_dict['users_organisations_table'], org_uuid,
                                                            user_uuid)
            can_user_be_demoted(cursor, org_uuid, user_uuid, target_user_uuid)
            demote_user(cursor, org_uuid, user_uuid, target_user_uuid)
            conn.commit()
    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to demote user'
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

    return {
        'statusCode': 200,
        'body': 'User Demoted Successfully'
    }