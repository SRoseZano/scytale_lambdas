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


def rename_organisation_address(cursor, addr_1,addr_2,city,county,postcode, user_uuid, org_uuid):
    try:
        get_entry = f"""
                                           SELECT * FROM {database_dict['schema']}.{database_dict['organisations_table']}
                                           WHERE organisationUUID = %s;
                           """
        cursor.execute(get_entry, (org_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        logging.info("Updating Org Address...")
        sql = f"UPDATE {database_dict['schema']}.{database_dict['organisations_table']} SET address_line_1 = %s, address_line_2 = %s, city = %s, county = %s, postcode = %s WHERE organisationUUID = %s "

        cursor.execute(sql, (addr_1,addr_2,city,county,postcode, org_uuid))

        sql_audit = sql % (addr_1,addr_2,city,county,postcode, org_uuid)

        cursor.execute(get_entry, (org_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['organisations_table'], 3, postcode, sql_audit,
            historic_row_json, current_row_json, org_uuid, user_uuid)

    except Exception as e:
        logging.error(f"Error updating org address: {e}")
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

        org_addr1_raw = body_json.get('addr1')
        org_addr2_raw = body_json.get('addr2')
        org_city_raw = body_json.get('city')
        org_county_raw = body_json.get('county')
        org_postcode_raw = body_json.get('postcode')

        variables = {
            'addr1': {'value': org_addr1_raw['value'], 'value_type': 'string_input'},
            'addr2': {'value': org_addr2_raw['value'], 'value_type': 'string_input'},
            'city': {'value': org_city_raw['value'], 'value_type': 'string_input'},
            'county': {'value': org_county_raw['value'], 'value_type': 'string_input'},
            'postcode': {'value': org_postcode_raw['value'], 'value_type': 'postcode_input'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        org_addr1 = variables['addr1']['value']
        org_addr2 = variables['addr2']['value']
        org_city = variables['city']['value']
        org_county = variables['county']['value']
        org_postcode = variables['postcode']['value']

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

            rename_organisation_address(cursor,  org_addr1, org_addr2, org_city, org_county, org_postcode, user_uuid, org_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to update organisations address'
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
        'body': 'Organisations Address Updated Successfully',
    }
