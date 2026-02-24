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

stripe_org_invoices_lambda = "GetStripeOrgInvoices"


def get_stripe_org_invoices(stripe_sub_id):
    try:
        logging.info("Creating stripe org invoices...")

        # Run policy creation lambda
        response = lambda_client.invoke(
            FunctionName=stripe_org_invoices_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps({'stripe_sub_id': stripe_sub_id})
        )

        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        logging.info(response_payload)

        if response['StatusCode'] != 200 or response_payload['statusCode'] != 200:
            logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
            traceback.print_exc()
            raise Exception(400, response_payload)

        return response_payload['invoices']

    except Exception as e:
        logging.error(f"Error getting subscription invoices: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_org_stripe_sub_id(cursor, org_uuid):
    try:
        logging.info("Getting stripe sub id...")
        sql = f"SELECT stripe_sub_id FROM {database_dict['schema']}.{database_dict['organisations_table']} WHERE organisationUUID = %s"

        cursor.execute(sql, (org_uuid,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError("Stripe subscription id doesn't exist for provided organisation id")

    except Exception as e:
        logging.error(f"Error fetching stripe sub id by orgUUID: {e}")
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

            stripe_sub_id = get_org_stripe_sub_id(cursor, org_uuid)
            if stripe_sub_id:
                invoices = get_stripe_org_invoices(stripe_sub_id)

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")

        status_value = 500
        body_value = 'Unable to get org invoices'
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
        'body': 'Org Invoices Returned Successfully',
        'invoices': invoices,
    }
