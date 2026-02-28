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

policy_detatch_lambda = "DetachPolicy"
remove_user_from_cognito_lambda = "DeleteAccountFromCognito"


def is_user_org_owner(cursor, schema, user_org_table, login_user_id, organisation_id):
    logging.info("Checking user permissions...")

    sql = f"""
        SELECT DISTINCT userUUID
        FROM {schema}.{user_org_table} a
        WHERE a.userUUID = %s
        AND a.organisationUUID = %s
        AND a.permissionid = 1
        LIMIT 1
    """

    cursor.execute(sql, (login_user_id, organisation_id))
    org_admin = cursor.fetchone()

    if org_admin:
        raise Exception(403,
                        "You are an owner of an organisation, assign new owner or delete organisation before deleting account")


def remove_user_from_cognito_pool(email):
    response = lambda_client.invoke(
        FunctionName=remove_user_from_cognito_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({"email": email})
    )

    response_payload = response['Payload'].read().decode('utf-8')

    if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
        raise Exception(response_payload)


def get_user_identities(cursor, user_uuid):
    logging.info("Fetching user identities...")
    sql = f"""
        SELECT DISTINCT a.identity_pool_id 
        FROM {database_dict['schema']}.{database_dict['users_table']} a
        WHERE userUUID = %s
    """
    cursor.execute(sql, (user_uuid,))
    user_identities = cursor.fetchall()
    user_identities = [identity[0] for identity in user_identities]
    return user_identities


def get_associated_policy(cursor, organisation_uuid):
    logging.info("Fetching associated policy...")

    sql = f"SELECT associated_policy FROM {database_dict['schema']}.{database_dict['organisations_table']} WHERE organisationUUID = %s;"
    cursor.execute(sql, (organisation_uuid,))
    result = cursor.fetchone()
    return result


def delete_user(cursor, org_uuid, user_uuid):
    logging.info("Deleting user...")

    sql = f"DELETE FROM {database_dict['schema']}.{database_dict['users_table']} WHERE userUUID = %s"
    cursor.execute(sql, (user_uuid,))


def detach_users_from_policy(lambda_client, policy_detatch_lambda, policy_name, user_identities):
    for user_identity in user_identities:

        response = lambda_client.invoke(
            FunctionName=policy_detatch_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps({"policy_name": policy_name, "user_identity": user_identity})
        )

        response_payload = response['Payload'].read().decode('utf-8')

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

        with conn.cursor() as cursor:

            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor, database_dict['schema'],
                                                                            database_dict['users_table'], user_email)
            org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor, database_dict['schema'],
                                                                               database_dict[
                                                                                   'users_organisations_table'],
                                                                               user_uuid)
            if org_uuid:
                is_user_org_owner(cursor, database_dict['schema'], database_dict['users_organisations_table'],
                                  user_uuid, org_uuid)
                user_identities = get_user_identities(cursor, org_uuid)
                policy_name, = get_associated_policy(cursor, org_uuid)

                delete_user(cursor, org_uuid, user_uuid)
                detach_users_from_policy(lambda_client, policy_detatch_lambda, policy_name, user_identities)

            remove_user_from_cognito_pool(user_email)

            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to delete user'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 403:  # if 422 then validation error
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
        'body': 'Account Deleted Successfully'
    }
