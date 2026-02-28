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

account_creation_lambda = "CreateAccount"
invite_creation_lambda = "InviteToOrganisation"
create_thing_lambda = "RegisterThing"


def create_hub_account(cursor, hubUUID):
    logging.info("Creating hub user account...")

    # Run policy creation lambda
    response = lambda_client.invoke(
        FunctionName=account_creation_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({})
    )

    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    logging.info(response_payload)

    if response['StatusCode'] != 200 or response_payload['statusCode'] != 200:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
        raise Exception(response_payload)

    logging.info("Setting Account As Hub...")

    sql = f"""
        UPDATE {database_dict['schema']}.{database_dict['users_table']} SET hub_user = 1 WHERE email = %s
    """
    cursor.execute(sql, (response_payload['body']['username'],))

    sql = f"""
                UPDATE {database_dict['schema']}.{database_dict['users_table']} SET hubUUID = %s WHERE email = %s
            """
    cursor.execute(sql, (hubUUID, response_payload['body']['username'],))

    return response_payload


def generate_hub_invite(auth_token):
    logging.info("Generating hub account organisation invite...")

    # Run policy creation lambda
    response = lambda_client.invoke(
        FunctionName=invite_creation_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({
            'params': {
                'header': {'Authorization': auth_token}
            },
            'body-json': {'invite_type_id': {'value': 3, 'value_type': 'id'}}
        })
    )

    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    logging.info(response_payload)

    if response['StatusCode'] != 200 or response_payload['statusCode'] != 200:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
        raise Exception(response_payload)

    return response_payload['code']


def create_hub(cursor, serial, registrant, hub_name, org_uuid, user_uuid):
    logging.info("Creating hub entry...")
    hub_uuid = zanolambdashelper.helpers.generate_time_based_uuid(user_uuid, hub_name)

    sql = f"INSERT INTO {database_dict['schema']}.{database_dict['hubs_table']} (hubUUID, serial, registrant, hub_name, organisationUUID, device_type_id, current_firmware) \
            VALUES (%s,%s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (hub_uuid, serial, registrant, hub_name, org_uuid, 1, '1.0.0'))

    return hub_uuid


def register_thing(thing_name, policy_name):
    logging.info("Creating hub as a thing...")

    # Run policy attach lambda
    response = lambda_client.invoke(
        FunctionName=create_thing_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({"thing_name": thing_name, "policy_name": policy_name})
    )

    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    logging.info(response_payload)

    if response['StatusCode'] != 200 or response_payload['statusCode'] != 200:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")

        raise Exception(response_payload)

    return response_payload


def retrieve_org_policy(cursor, organisation_uuid):
    logging.info("Retrieving IoT policy name... ")
    # Fetch associated policy and organisation UUID
    sql = f"SELECT associated_policy FROM {database_dict['organisations_table']} WHERE organisationUUID = %s;"
    cursor.execute(sql, (organisation_uuid,))
    result = cursor.fetchone()

    return result


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        hub_name_raw = body_json.get('hub_name')
        serial_raw = body_json.get('serial')

        variables = {
            'hub_name': {'value': hub_name_raw['value'], 'value_type': 'string_input'},
            'serial': {'value': serial_raw['value'], 'value_type': 'mac_address'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        hub_name = variables['hub_name']['value']
        serial = variables['serial']['value']

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
            policy_name, = retrieve_org_policy(cursor, org_uuid)
            hub_uuid = create_hub(cursor, serial, user_email, hub_name, org_uuid, user_uuid)
            account_details = create_hub_account(cursor, hub_uuid)
            invite_code = generate_hub_invite(auth_token)
            certs = register_thing(hub_uuid, policy_name)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to register hub'
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
        'body': 'Invite Generated Successfully',
        'username': account_details['body']['username'],
        'password': account_details['body']['password'],
        'inviteCode': invite_code,
        'thingName': certs.get('body', {}).get('thingName', ''),
        'certificateId': certs.get('body', {}).get('certificateId', ''),
        'certificatePem': certs.get('body', {}).get('certificatePem', ''),
        'privateKey': certs.get('body', {}).get('privateKey', ''),
        'hub_topic': hub_uuid
    }
