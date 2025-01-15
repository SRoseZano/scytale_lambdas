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

rds_client =  zanolambdashelper.helpers.create_client('rds') 
lambda_client = zanolambdashelper.helpers.create_client('lambda')

zanolambdashelper.helpers.set_logging('INFO')

policy_creation_lambda = "CreatePolicy"
policy_attach_lambda = "AttachPolicy"


def is_in_org(cursor, schema, user_organisations_intersect_table, login_user_id):
    try:
        logging.info("Checking user permissions...")
        sql = f"SELECT DISTINCT userID FROM {database_dict['schema']}.{database_dict['users_organisations_table']} WHERE userid = %s LIMIT 1;"
        cursor.execute(sql, (login_user_id,))
        in_org = cursor.fetchone()
        return in_org is not None
    except Exception as e:
        logging.error(f"Error checking user permissions: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_organisation(cursor, organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number):
    try:
        logging.info("Creating Organisation...")
        # Create organisation entry in database
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['organisations_table']} (organisation_name, associated_policy, address_line_1, address_line_2, city, county, postcode, phone_no) \
        VALUES (%s, CONCAT('Policy_', UUID()), %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number))

        # Fetch the last inserted ID
        cursor.execute("SELECT LAST_INSERT_ID();")
        organisation_id = cursor.fetchone()[0]
        logging.info(f"Organisation created with ID: {organisation_id}")
        return organisation_id
    except Exception as e:
        logging.error(f"Error creating organisation entry: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_user_organisation_relation(cursor, login_user_id, organisation_id):
    try:
        logging.info("Adding User To New Organisation...")
        # Create user_organisations entry in database
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userid, organisationid, permissionid) VALUES (%s, %s, 1);"
        cursor.execute(sql, (login_user_id, organisation_id))
        logging.info("User organisation relation created")

    except Exception as e:
        logging.error(f"Error creating user organisation relation: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_default_pool(cursor, organisation_id, organisation_name):
    try:
        logging.info("Creating new organisation default pool...")
        # Create default pool entry in database
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['pools_table']} (organisationid, pool_name, parentid) VALUES (%s, %s, NULL)"
        cursor.execute(sql, (organisation_id, f"{organisation_name} Default Pool"))

        # Fetch the last inserted ID
        cursor.execute("SELECT LAST_INSERT_ID();")
        pool_id = cursor.fetchone()[0]
        logging.info(f"Default pool created with ID: {pool_id}")
        return pool_id
    except Exception as e:
        logging.error(f"Error creating default pool entry: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def add_user_to_pool(cursor, pool_id, login_user_id):
    logging.info("Adding user to newly created default pool...")
    try:
        # Create pools_users entry in database
        sql = f"INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (poolid, userid) VALUES (%s, %s);"
        cursor.execute(sql, (pool_id, login_user_id))
        logging.info("User added to pool")
    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def update_user_identity_pool(cursor, user_identity, login_user_id):
    logging.info("Setting users identity_pool_id...")
    try:
        # Update the user entry to include the identity pool ID
        sql = f"UPDATE {database_dict['schema']}.{database_dict['users_table']} SET identity_pool_id = %s WHERE userID = %s"
        cursor.execute(sql, (user_identity, login_user_id))
        logging.info("User identity pool updated")
    except Exception as e:
        logging.error(f"Error updating user identity pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_and_attach_policy(cursor, organisation_id, organisation_name, policy_creation_lambda, policy_attach_lambda, user_identity):
    try:
        logging.info("Creating and Attatching IoT policy to organisation...")
        # Fetch associated policy and organisation UUID
        sql = f"SELECT associated_policy, organisationUUID FROM {database_dict['organisations_table']} WHERE organisationid = %s;"
        cursor.execute(sql, (organisation_id,))
        result = cursor.fetchone()
        policy_name = result[0]
        organisation_uuid = result[1]

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
            traceback.print_exc()
            raise Exception(400, {response_payload})


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
            traceback.print_exc()
            raise Exception(400, e)

    except Exception as e:
        logging.error(f"Error creating and attaching policy: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port, rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user,database_token,rds_db,rds_host,rds_port)
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
            'organisation_name': {'value': organisation_name_raw['value'], 'value_type': organisation_name_raw['value_type']},
            'address_line_1': {'value': address_line_1_raw['value'], 'value_type': address_line_1_raw['value_type']},
            'city': {'value': city_raw['value'], 'value_type': city_raw['value_type']},
            'county': {'value': county_raw['value'], 'value_type': county_raw['value_type']},
            'postcode': {'value': postcode_raw['value'], 'value_type': postcode_raw['value_type']},
            'phone_number': {'value': phone_number_raw['value'], 'value_type': phone_number_raw['value_type']},
            'user_identity': {'value': user_identity_raw['value'], 'value_type': user_identity_raw['value_type']}
        }
        
        if address_line_2_raw['value']: #add optionals if exists
            variables['address_line_2'] = {'value': address_line_2_raw['value'], 'value_type': address_line_2_raw['value_type']}
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        organisation_name = variables['organisation_name']['value']
        address_line_1 = variables['address_line_1']['value']
        address_line_2 = variables['address_line_2']['value'] if address_line_2_raw['value'] else None
        city = variables['city']['value']
        county = variables['county']['value']
        postcode = variables['postcode']['value']
        phone_number = variables['phone_number']['value']
        user_identity = variables['user_identity']['value']


        with conn.cursor() as cursor:
            login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            
            if not is_in_org(cursor, database_dict['schema'], database_dict['users_organisations_table'], login_user_id):
                
                # Create organisation entry in database
                organisation_id = create_organisation(cursor, organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number)

                # Create user_organisations entry in database
                create_user_organisation_relation(cursor, login_user_id, organisation_id)

                # Create default pool entry in database
                pool_id = create_default_pool(cursor, organisation_id, organisation_name)

                # Add user to pool
                add_user_to_pool(cursor, pool_id, login_user_id)

                # Update user identity pool
                update_user_identity_pool(cursor, user_identity, login_user_id)

                # Create and attach policy
                create_and_attach_policy(cursor, organisation_id, organisation_name, policy_creation_lambda, policy_attach_lambda, user_identity)


                conn.commit()
    
            else:
                traceback.print_exc()
                raise Exception(400)


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation 
            body_value = e.args[1]
        else:
            body_value = 'Unable to create organisation'
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

