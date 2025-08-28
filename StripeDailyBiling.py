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
import stripe
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

stripe_sub_update_lambda = "UpdateStripeSubscriptions"


def get_valid_org_subs(cursor):
    logging.info("Getting all org subscriptions and device counts...")
    try:
        sql = f"""
                WITH cte as (
                    SELECT organisationUUID, device_type_ID from {database_dict['schema']}.{database_dict['hubs_table']}  
                    UNION ALL 
                    SELECT organisationUUID, device_type_ID from {database_dict['schema']}.{database_dict['devices_table']}
                    )
                SELECT a.organisationUUID, a.stripe_sub_id,
                CAST(SUM(CASE WHEN device_type_ID = 1 THEN 1 ELSE 0 END) AS UNSIGNED) AS hub_count,
                CAST(SUM(CASE WHEN device_type_ID = 2 THEN 1 ELSE 0 END) AS UNSIGNED) AS dimmable_light_count,
                CAST(SUM(CASE WHEN device_type_ID = 3 THEN 1 ELSE 0 END) AS UNSIGNED) AS encoder_count,
                CAST(SUM(CASE WHEN device_type_ID = 4 THEN 1 ELSE 0 END) AS UNSIGNED) AS pir_count,
                CAST(SUM(CASE WHEN device_type_ID = 5 THEN 1 ELSE 0 END) AS UNSIGNED) AS emergency_light_count
                FROM {database_dict['schema']}.{database_dict['organisations_table']}  a LEFT JOIN cte b on a.organisationUUID = b.organisationUUID
                WHERE stripe_sub_id IS NOT NULL
                GROUP BY a.organisationUUID, a.stripe_sub_id
        """
        cursor.execute(sql)

        org_sub_result = cursor.fetchall()

        if org_sub_result:
            colnames = [desc[0] for desc in cursor.description]
            return {row[1]: dict(zip(colnames, row)) for row in org_sub_result}
        else:
            return {}

    except Exception as e:
        logging.error(f"Error getting organisation subs: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def update_stripe_sub(org_subs):
    try:
        logging.info("Logging subscription usage...")

        response = lambda_client.invoke(
            FunctionName=stripe_sub_update_lambda,
            InvocationType="RequestResponse",
            Payload=json.dumps(org_subs)
        )

        response_payload_str = response['Payload'].read().decode('utf-8')
        logging.info(response_payload_str)
        try:
            response_payload = json.loads(response_payload_str)
        except json.JSONDecodeError:
            response_payload = {}

        if response['StatusCode'] != 200 or response_payload.get(
                'statusCode') != 200 or 'errorMessage' in response_payload:
            logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
            traceback.print_exc()
            raise Exception(400, response_payload)

    except Exception as e:
        logging.error(f"Error updating subscription: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        with conn.cursor() as cursor:

            org_subs = get_valid_org_subs(cursor)
            update_stripe_sub(org_subs)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to log org subscription useage'
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
        'body': 'Org subscriptions updated successfully'
    }

