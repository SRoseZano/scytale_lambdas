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

policy_creation_lambda = "CreatePolicy"
policy_attach_lambda = "AttachPolicy"


def is_in_org(cursor, login_user_uuid):
    logging.info("Checking user permissions...")

    sql = f"SELECT DISTINCT userUUID FROM {database_dict['schema']}.{database_dict['users_organisations_table']} WHERE userUUID = %s LIMIT 1;"
    cursor.execute(sql, (login_user_uuid,))
    result = cursor.fetchone()

    if result is not None:
        raise Exception(412, f"User already belongs to an organisation")

    return result


def create_organisation(cursor, organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number,
                        user_uuid):
    org_uuid = zanolambdashelper.helpers.generate_time_based_uuid(user_uuid, organisation_name)

    logging.info("Creating Organisation...")

    sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['organisations_table']} 
            (organisationUUID,organisation_name, associated_policy, address_line_1, address_line_2, city, county, postcode, phone_no) 
            VALUES (%s,%s, CONCAT('Policy_', %s), %s, %s, %s, %s, %s, %s)
        """
    cursor.execute(sql,
                   (org_uuid, organisation_name, org_uuid, address_line_1, address_line_2, city, county, postcode,
                    phone_number))

    return org_uuid


def create_user_organisation_relation(cursor, user_uuid, org_uuid):
    logging.info("Adding User To New Organisation...")

    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} 
        (userUUID, organisationUUID, permissionid) 
        VALUES (%s, %s, 1);
    """
    cursor.execute(sql, (user_uuid, org_uuid))


def create_default_pool(cursor, organisation_name, org_uuid, user_uuid):
    logging.info("Creating new organisation default pool...")

    pool_uuid = zanolambdashelper.helpers.generate_time_based_uuid(user_uuid, organisation_name)

    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['pools_table']} 
        (poolUUID,organisationUUID, pool_name, parentUUID) 
        VALUES (%s,%s, %s, NULL)
    """
    cursor.execute(sql, (pool_uuid, org_uuid, f"{organisation_name} Default Pool"))

    return pool_uuid


def add_user_to_pool(cursor, pool_uuid, org_uuid, user_uuid):
    logging.info("Adding user to newly created default pool...")

    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} 
        (poolUUID, userUUID) 
        VALUES (%s, %s);
    """
    cursor.execute(sql, (pool_uuid, user_uuid))


def update_user_identity_pool(cursor, user_identity, org_uuid, user_uuid):
    logging.info("Setting user's identity_pool_id...")

    sql = f"""
        UPDATE {database_dict['schema']}.{database_dict['users_table']} 
        SET identity_pool_id = %s 
        WHERE userUUID = %s
    """
    cursor.execute(sql, (user_identity, user_uuid))


def create_and_attach_policy(cursor, org_uuid, policy_creation_lambda, policy_attach_lambda,
                             user_identity):
    logging.info("Creating and Attatching IoT policy to organisation...")
    # Fetch associated policy and organisation UUID
    sql = f"SELECT associated_policy FROM {database_dict['organisations_table']} WHERE organisationUUID = %s;"
    cursor.execute(sql, (org_uuid,))
    result = cursor.fetchone()
    policy_name = result[0]
    organisation_uuid = org_uuid

    # Run policy creation lambda
    response = lambda_client.invoke(
        FunctionName=policy_creation_lambda,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps({"policy_name": policy_name, "organisation_UUID": organisation_uuid})
    )
    logging.info("Policy created")

    response_payload = response['Payload'].read().decode('utf-8')
    logging.info(response_payload)

    if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
        logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
        raise Exception(response_payload)

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
        organisation_name_raw = body_json.get('organisation_name')
        address_line_1_raw = body_json.get('address_line_1')
        address_line_2_raw = body_json.get('address_line_2', None)
        city_raw = body_json.get('city')
        county_raw = body_json.get('county')
        postcode_raw = body_json.get('postcode')
        phone_number_raw = body_json.get('phone_number')
        user_identity_raw = body_json.get('user_identity')

        variables = {
            'organisation_name': {'value': organisation_name_raw['value'],
                                  'value_type': 'string_input'},
            'address_line_1': {'value': address_line_1_raw['value'], 'value_type': 'string_input'},
            'city': {'value': city_raw['value'], 'value_type': 'string_input'},
            'county': {'value': county_raw['value'], 'value_type': 'string_input'},
            'postcode': {'value': postcode_raw['value'], 'value_type': 'postcode_input'},
            'phone_number': {'value': phone_number_raw['value'], 'value_type': 'phone_number_input'},
            'user_identity': {'value': user_identity_raw['value'], 'value_type': 'string_input'}
        }

        if address_line_2_raw['value']:  # add optionals if exists
            variables['address_line_2'] = {'value': address_line_2_raw['value'],
                                           'value_type': 'string_input'}

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        organisation_name = variables['organisation_name']['value']
        address_line_1 = variables['address_line_1']['value']
        address_line_2 = variables['address_line_2']['value'] if address_line_2_raw['value'] else None
        city = variables['city']['value']
        county = variables['county']['value']
        postcode = variables['postcode']['value']
        phone_number = variables['phone_number']['value']
        user_identity = variables['user_identity']['value']

        with conn.cursor() as cursor:
            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor,
                                                                            database_dict['schema'],
                                                                            database_dict['users_table'],
                                                                            user_email)
            # check user isnt already in org
            is_in_org(cursor, user_uuid)

            # Create organisation entry in database
            org_uuid = create_organisation(cursor, organisation_name, address_line_1,
                                           address_line_2, city, county, postcode, phone_number,
                                           user_uuid)

            # Create user_organisations entry in database
            create_user_organisation_relation(cursor, user_uuid, org_uuid)

            # Create default pool entry in database
            pool_uuid = create_default_pool(cursor, organisation_name, org_uuid, user_uuid)

            # Add user to pool
            add_user_to_pool(cursor, pool_uuid, org_uuid, user_uuid)

            # Update user identity pool
            update_user_identity_pool(cursor, user_identity, org_uuid, user_uuid)

            # Create and attach policy
            create_and_attach_policy(cursor, org_uuid, policy_creation_lambda,
                                     policy_attach_lambda, user_identity)

            conn.commit()


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to create organisation'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 412:  # if 422 then validation error
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
        'body': 'Organisation Created Successfully'
    }

