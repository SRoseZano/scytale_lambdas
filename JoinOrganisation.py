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
lambda_client = zanolambdashelper.helpers.create_client('lambda')

zanolambdashelper.helpers.set_logging('INFO')

policy_attach_lambda = "AttachPolicy"


def get_user_and_hub_id_by_email(cursor,
                                 user_email):  # not using helper get id function because this one also requires hubid for join logic

    logging.info("Getting user details...")

    sql = f"SELECT hub_user, userUUID FROM {database_dict['schema']}.{database_dict['users_table']} WHERE email = %s"
    cursor.execute(sql, (user_email,))
    result = cursor.fetchone()
    if result:
        return result
    else:
        raise Exception("UserUUID doesn't exist for provided user email")


def join_organisation(cursor, invite_code, login_user_hub, user_uuid):
    logging.info("Joining Organisation...")

    if login_user_hub == 1:  # if joiner is hub user then dont worry about invite expiry
        get_organisation_uuid_sql = f""" SELECT DISTINCT organisationUUID, inviteID FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE invite_code = %s LIMIT 1 """
    else:
        get_organisation_uuid_sql = f""" SELECT DISTINCT organisationUUID, inviteID FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE invite_code = %s AND valid_until >= NOW() LIMIT 1 """

    cursor.execute(get_organisation_uuid_sql, (invite_code,))

    get_organisation_uuid_sql_result = cursor.fetchone()

    if get_organisation_uuid_sql_result:
        logging.info("OrganisationUUID found")
        if get_organisation_uuid_sql_result[1] == 3 and login_user_hub == 1:  # if invite type is hub and the
            join_organisation_sql = f""" INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userUUID, organisationUUID, permissionid) VALUES (%s, %s, 2);"""
        else:
            join_organisation_sql = f""" INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userUUID, organisationUUID, permissionid) VALUES (%s, %s, 3);"""

        cursor.execute(join_organisation_sql, (user_uuid, get_organisation_uuid_sql_result[0]))

        return get_organisation_uuid_sql_result[0]
    else:
        raise Exception("Unable to retrive invite code")


def configure_mqtt(cursor, user_identity, org_uuid, user_uuid):
    logging.info("Configuring org policy to user identity...")

    update_user_identity_pool(cursor, user_identity, org_uuid, user_uuid)
    attach_policy(cursor, org_uuid, user_identity)


def update_user_identity_pool(cursor, user_identity, org_uuid, user_uuid):
    logging.info("Setting users identity_pool_id...")

    # Update the user entry to include the identity pool ID
    sql = f"UPDATE {database_dict['schema']}.{database_dict['users_table']} SET identity_pool_id = %s WHERE userUUID = %s"

    cursor.execute(sql, (user_identity, user_uuid))


def attach_policy(cursor, org_uuid, user_identity):
    logging.info("Attatching IoT policy to user identity...")

    # Fetch associated policy and organisation UUID
    sql = f"SELECT associated_policy FROM {database_dict['organisations_table']} WHERE organisationUUID = %s;"
    cursor.execute(sql, (org_uuid,))
    result = cursor.fetchone()
    policy_name = result[0]

    # Run policy attach lambda
    response = lambda_client.invoke(
        FunctionName=policy_attach_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({"policy_name": policy_name, "user_identity": user_identity})
    )
    logging.info("Policy attached")

    response_payload = response['Payload'].read().decode('utf-8')
    logging.info(response_payload)

    if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
        raise Exception(response_payload)


def append_user_to_all_pools(cursor, org_uuid, user_uuid):
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


def append_user_to_default_pool(cursor, org_uuid, user_uuid):
    logging.info("Executing SQL query to append user to all org pools...")

    # SQL query to find top level pool and assign to everyone under it
    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userUUID, poolUUID)
        SELECT %s AS userUUID, poolUUID
        FROM {database_dict['schema']}.{database_dict['pools_table']}
        WHERE parentUUID IS NULL AND organisationUUID = %s;

    """

    cursor.execute(sql, (user_uuid, org_uuid,))


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        # Extract relevant attributes if non existant set empty
        invite_code_raw = body_json.get('invite_code')
        user_identity_raw = body_json.get('user_identity')

        variables = {
            'invite_code': {'value': invite_code_raw['value'], 'value_type': 'string_input'},
            'user_identity': {'value': user_identity_raw['value'], 'value_type': 'string_input'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        invite_code = variables['invite_code']['value']
        user_identity = variables['user_identity']['value']

        with conn.cursor() as cursor:

            login_user_hub, user_uuid = get_user_and_hub_id_by_email(cursor, user_email)

            org_uuid = join_organisation(cursor, invite_code, login_user_hub,
                                         user_uuid)

            if (login_user_hub == 1):  # if new user is hub add user to all pools (for hub get org details )
                append_user_to_all_pools(cursor, org_uuid, user_uuid)
            else:
                append_user_to_default_pool(cursor, org_uuid, user_uuid)

            configure_mqtt(cursor, user_identity, org_uuid, user_uuid)

            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to join organisation'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 402:  # if 422 then validation error
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
        'body': 'Joined Organisation Successfully',

    }
