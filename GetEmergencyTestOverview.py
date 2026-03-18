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

max_files_per_year = 10
output_bucket = "scytale-prod-emergency-test-reports-423623864387-eu-west-2-an"

s3 = zanolambdashelper.helpers.create_client('s3')

def get_org_test_result_overview(org_uuid):
    logging.info("Creating stripe org invoices...")

    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{org_uuid}/"
    files_by_year = {}

    # Paginate through all objects under the org_uuid prefix
    for page in paginator.paginate(Bucket=output_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Skip folders
            if key.endswith("/"):
                continue

            parts = key.split("/")
            if len(parts) < 2:
                continue
            year = parts[1]
            filename = parts[-1]

            files_by_year.setdefault(year, []).append(filename)

    # Sort files per year by filename descending and keep only most recent 10
    for year in files_by_year:
        files_by_year[year].sort(reverse=True)  # newest first
        files_by_year[year] = files_by_year[year][:max_files_per_year]

    return files_by_year


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



            results = get_org_test_result_overview(org_uuid)

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to get org test results'
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
        'body': 'Org test result overview returned successfully',
        'results': results,
    }
