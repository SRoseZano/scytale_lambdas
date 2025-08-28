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


def lambda_handler(event, context):
    database_token = rds_client.generate_db_auth_token(
        DBHostname=rds_host,
        Port=rds_port,
        DBUsername=rds_user,
        Region=os.environ['AWS_REGION']
    )

    conn = mysql.connector.connect(user=rds_user, password=database_token, host=rds_host, database=rds_db,
                                   port=rds_port)
    conn.autocommit = False

    # Extract user details from Cognito event
    user_attributes = event['request']['userAttributes']

    print(user_attributes);

    # Extract relevant attributes if non existant set empty have to set up types here as they cannot parse to argument
    email = user_attributes.get('email')
    birthdate = user_attributes.get('birthdate')
    zoneinfo = user_attributes.get('zoneinfo')
    locale = user_attributes.get('locale')
    first_name = user_attributes.get('custom:first_name')
    last_name = user_attributes.get('custom:last_name')

    variables = {
        'email': {'value': user_attributes.get('email'), 'value_type': 'email_input'},
        'birthdate': {'value': user_attributes.get('birthdate'), 'value_type': 'birthdate_input'},
        'zoneinfo': {'value': user_attributes.get('zoneinfo'), 'value_type': 'string_input'},
        'locale': {'value': user_attributes.get('locale'), 'value_type': 'string_input'},
        'first_name': {'value': user_attributes.get('custom:first_name'), 'value_type': 'name_input'},
        'last_name': {'value': user_attributes.get('custom:last_name'), 'value_type': 'name_input'},
    }

    logging.info("Validating and cleansing user inputs...")
    variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

    email = variables['email']['value']
    birthdate = variables['birthdate']['value']
    zoneinfo = variables['zoneinfo']['value']
    locale = variables['locale']['value']
    first_name = variables['first_name']['value']
    last_name = variables['last_name']['value']

    try:
        with conn.cursor() as cursor:
            user_uuid = zanolambdashelper.helpers.generate_time_based_uuid(first_name, email)
            sql = f"""
                INSERT INTO {database_dict['schema']}.{database_dict['users_table']} 
                (userUUID, email, first_name, last_name, birthdate, zone_info, locale) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(sql, (user_uuid, email, first_name, last_name, birthdate, zoneinfo, locale))
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        elif isinstance(e, mysql.connector.Error) and e.errno == 1062:
            body_value = "An account already exists with this email"
        else:
            body_value = 'Unable to create account'

        raise Exception(body_value)

    finally:
        try:
            cursor.close()
            conn.close()
        except NameError:  # catch potential error before cursor or conn is defined
            pass

    return event
