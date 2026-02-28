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

policy_detach_lambda = "DetachPolicy"

zanolambdashelper.helpers.set_logging('INFO')


def get_org_owner_count(cursor, organisation_uuid, user_uuid):
    logging.info("Executing SQL query to check amount of owners in organisation other than self...")

    sql = f"""
        SELECT COUNT(DISTINCT a.userUUID)
        FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
        JOIN {database_dict['schema']}.{database_dict['users_table']} b ON a.userUUID = b.userUUID
        WHERE a.permissionID = 1 AND a.organisationUUID = %s and b.hub_user = 0 AND a.userUUID <> %s
        """

    cursor.execute(sql, (organisation_uuid, user_uuid,))
    count, = cursor.fetchone()

    return count


def has_permissions_to_remove_target(cursor, user_uuid, target_user_uuid, org_uuid):
    logging.info("Checking login user permissions...")

    sql = f"""
        SELECT DISTINCT permissionid
        FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
        WHERE a.userUUID = %s
        AND a.organisationUUID = %s
        LIMIT 1
    """

    cursor.execute(sql, (user_uuid, org_uuid))
    login_user_permissions, = cursor.fetchone()

    logging.info("Checking target user permissions...")

    sql = f"""
        SELECT DISTINCT permissionid
        FROM {database_dict['schema']}.{database_dict['users_organisations_table']} a
        WHERE a.userUUID = %s
        AND a.organisationUUID = %s
        LIMIT 1
    """

    cursor.execute(sql, (target_user_uuid, org_uuid))
    target_user_permissions, = cursor.fetchone()

    if login_user_permissions >= target_user_permissions:
        raise Exception(402, "Cannot remove a user of same permission status from group, please demote user first")


def remove_user_from_organisation(cursor, org_uuid, user_uuid):
    logging.info("Executing SQL query to remove user from organisation...")

    sql = f"""
        DELETE c
        FROM {database_dict['schema']}.{database_dict['users_organisations_table']} c
        WHERE c.userUUID = %s AND c.organisationUUID = %s
        """
    cursor.execute(sql, (user_uuid, org_uuid))

    logging.info("Executing SQL query to remove user from any of the organisation pools...")

    sql = f"""
        DELETE p
        FROM {database_dict['schema']}.{database_dict['pools_users_table']} p 
        INNER JOIN {database_dict['schema']}.{database_dict['pools_table']} a ON p.poolUUID = a.poolUUID 
        WHERE p.userUUID = %s AND a.organisationUUID = %s
        """
    cursor.execute(sql, (user_uuid, org_uuid))


def retrieve_org_policy(cursor, org_uuid):
    logging.info("Retrieving IoT policy name... ")

    # Fetch associated policy and organisation UUID
    sql = f"SELECT associated_policy FROM {database_dict['organisations_table']} WHERE organisationUUID = %s;"
    cursor.execute(sql, (org_uuid,))

    result = cursor.fetchone()
    policy_name, = result

    return policy_name


def retrieve_user_identity(cursor, user_uuid):
    logging.info("Retrieving users identity... ")

    sql = f"SELECT identity_pool_id FROM {database_dict['users_table']} WHERE userUUID = %s;"
    cursor.execute(sql, (user_uuid,))
    result = cursor.fetchone()
    identity, = result

    return identity


def detach_org_policy(cursor, org_uuid, user_uuid):
    policy_name = retrieve_org_policy(cursor, org_uuid)
    user_identity = retrieve_user_identity(cursor, user_uuid)

    logging.info("Detaching IoT policy to user identity...")

    # Run policy attach lambda
    response = lambda_client.invoke(
        FunctionName=policy_detach_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({"policy_name": policy_name, "user_identity": user_identity})
    )
    logging.info("Policy detached")

    response_payload = response['Payload'].read().decode('utf-8')
    logging.info(response_payload)

    if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
        raise Exception(response_payload)


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

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
            zanolambdashelper.helpers.is_user_org_admin(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], user_uuid,
                                                        org_uuid)
            zanolambdashelper.helpers.is_target_user_in_org(cursor, database_dict['schema'],
                                                            database_dict['users_organisations_table'], org_uuid,
                                                            target_user_uuid)

            if get_org_owner_count(cursor,
                                   org_uuid, target_user_uuid) == 0:  # if removing yourself would leave 0 owners block
                logging.error(
                    f"Unable to leave organisation as last owner, either promote another user to owner or delete your organisation")
                raise Exception(403,
                                "Unable to leave organisation as last owner, either promote another user to owner or delete your organisation")

            if user_uuid != target_user_uuid:  # if removing another user check you have the correct permissions
                has_permissions_to_remove_target(cursor, user_uuid, target_user_uuid, org_uuid)

            remove_user_from_organisation(cursor, org_uuid, target_user_uuid)
            detach_org_policy(cursor, org_uuid, target_user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        result = cursor.fetchone()
        status_value = 500
        body_value = 'Unable to remove user from organisation'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 403 or status_value == 402:  # if 422 then validation error
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
        'body': 'User Removed From Organisation Successfully'
    }
