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


def rename_hub(cursor, organisation_id, hub_name, hub_id, org_uuid, user_uuid):
    try:
        get_entry = f"""
                                           SELECT * FROM {database_dict['schema']}.{database_dict['hubs_table']}
                                           WHERE organisationid = %s AND hubid = %s;
                           """
        cursor.execute(get_entry, (organisation_id, hub_id,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        logging.info("Creating pool...")
        sql = f"UPDATE {database_dict['schema']}.{database_dict['hubs_table']} SET hub_name = %s WHERE organisationid = %s AND hubid = %s "

        cursor.execute(sql, (hub_name, organisation_id, hub_id))

        sql_audit = sql % (hub_name, organisation_id, hub_id)

        cursor.execute(get_entry, (organisation_id, hub_id,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['hubs_table'], 3, hub_id, sql_audit,
            historic_row_json, current_row_json, org_uuid, user_uuid)

    except Exception as e:
        logging.error(f"Error updating pool name: {e}")
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

        hub_name_raw = body_json.get('hub_name')
        hub_id_raw = body_json.get('hub_id')

        variables = {
            'hub_name': {'value': hub_name_raw['value'], 'value_type': hub_name_raw['value_type']},
            'hub_id': {'value': hub_id_raw['value'], 'value_type': hub_id_raw['value_type']},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        hub_name = variables['hub_name']['value']
        hub_id = variables['hub_id']['value']

        with conn.cursor() as cursor:

            login_user_id, user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor,
                                                                                           database_dict['schema'],
                                                                                           database_dict['users_table'],
                                                                                           user_email)
            organisation_id, org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor,
                                                                                                database_dict['schema'],
                                                                                                database_dict[
                                                                                                    'users_organisations_table'],
                                                                                                login_user_id)
            print(organisation_id)

            print(database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)
            # validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], login_user_id,
                                                        organisation_id)
            zanolambdashelper.helpers.is_target_hub_in_org(cursor, database_dict['schema'],
                                                            database_dict['hubs_table'], organisation_id, hub_id)

            rename_hub(cursor, organisation_id, hub_name, hub_id, org_uuid, user_uuid)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to update hub name'
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
        'body': 'Hub Name Updated Successfully',
    }
