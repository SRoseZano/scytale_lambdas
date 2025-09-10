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

delete_account_from_cognito_lambda = "DeleteHubAccountsFromCognito"
delete_thing_from_iot_core_lambda = "DeleteThingFromIOTCore"
policy_detatch_lambda = "DetachPolicy"


def get_hub_uuids(cursor):
    try:
        logging.info("Fetching hub uuids...")
        sql = f"""
            SELECT DISTINCT a.hubUUID 
            FROM {database_dict['schema']}.{database_dict['hubs_table']} a
        """
        cursor.execute(sql)
        result_hub_uuids = cursor.fetchall()
        hub_uuids = [hub_uuid[0] for hub_uuid in result_hub_uuids]
        return hub_uuids
    except Exception as e:
        logging.error(f"Error fetching hub uuids: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def get_hub_accounts(cursor):
    try:
        logging.info("Fetching hub accounts...")

        sql = f"""
            SELECT DISTINCT a.email, a.identity_pool_id, d.associated_policy
            FROM {database_dict['schema']}.{database_dict['users_table']} a
            LEFT JOIN {database_dict['schema']}.{database_dict['hubs_table']} b
                ON a.hubUUID = b.hubUUID
            INNER JOIN {database_dict['schema']}.{database_dict['users_organisations_table']} c
                ON a.userUUID = c.userUUID
            INNER JOIN {database_dict['schema']}.{database_dict['organisations_table']} d 
                ON c.organisationUUID = d.organisationUUID
            WHERE a.first_name = 'John' 
            AND a.last_name = 'Doe' 
            AND a.email LIKE '%@zanocontrols.co.uk'
            AND b.hubUUID IS NULL
        """

        cursor.execute(sql)
        result = cursor.fetchall()

        if not result:
            logging.info("No hub accounts found.")
            return [], []

        emails = []
        email_identity_policy_tuples = []

        for email, identity_pool_id, associated_policy in result:
            emails.append(email)
            email_identity_policy_tuples.append((email,identity_pool_id, associated_policy))

        return emails, email_identity_policy_tuples

    except Exception as e:
        logging.error(f"Error fetching hub accounts: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def delete_hub_entries_from_db(cursor, emails):
    try:
        if not emails:
            logging.info("No emails to delete.")
            return

        logging.info("Deleting hub accounts from db...")

        placeholders = ','.join(['%s'] * len(emails))
        sql = f"""
            DELETE FROM {database_dict['schema']}.{database_dict['users_table']}
            WHERE email IN ({placeholders})
        """

        cursor.execute(sql, tuple(emails))

    except Exception as e:
        logging.error(f"Error deleting hub accounts from db: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def delete_users_from_cognito(lambda_client, emails):
    try:
        response = lambda_client.invoke(
            FunctionName=delete_account_from_cognito_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps({"users": emails})
        )

        response_payload = response['Payload'].read().decode('utf-8')

        if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
            logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
            traceback.print_exc()
            raise Exception(400, f"Lambda invocation failed, ResponsePayload: {response_payload}")

    except Exception as e:
        logging.error(f"Error removing users from cognito pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def delete_users_from_iotcore(lambda_client, hub_uuids):
    try:
        response = lambda_client.invoke(
            FunctionName=delete_thing_from_iot_core_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps({"things": hub_uuids})
        )

        response_payload = response['Payload'].read().decode('utf-8')

        # Convert JSON string to Python dictionary
        payload_dict = json.loads(response_payload)

        if response['StatusCode'] != 200 or payload_dict.get("statusCode") != 200 or 'errorMessage' in response_payload:
            logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
            traceback.print_exc()
            raise Exception(400, f"Lambda invocation failed, ResponsePayload: {response_payload}")

    except Exception as e:
        logging.error(f"Error removing hubs from IoT core: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def detach_users_from_policy(lambda_client, user_identities):
    try:
        logging.info("Detaching hub users from their policies...")

        for identity_pool_id, policy_name in user_identities:
            logging.info(f"Detaching identity '{identity_pool_id}' from policy '{policy_name}'")

            response = lambda_client.invoke(
                FunctionName=policy_detatch_lambda,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=json.dumps({
                    "policy_name": policy_name,
                    "user_identity": identity_pool_id
                })
            )

            response_payload = response['Payload'].read().decode('utf-8')

            if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
                logging.error(f"Lambda invocation failed for identity {identity_pool_id}. Response: {response_payload}")
                traceback.print_exc()
                raise Exception(400, f"Lambda invocation failed, ResponsePayload: {response_payload}")

    except Exception as e:
        logging.error(f"Error detaching users from policy: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        with conn.cursor() as cursor:

            hub_uuids = get_hub_uuids(cursor)
            hub_emails, hub_email_policy_identity_pairs = get_hub_accounts(cursor)
            delete_hub_entries_from_db(cursor, hub_emails)
            delete_users_from_cognito(lambda_client, hub_emails)
            delete_users_from_iotcore(lambda_client, hub_uuids)

            # Filter to get only the identity pairs for deleted emails
            policy_identity_pairs_to_delete = [
                (identity_pool_id, associated_policy)
                for _, identity_pool_id, associated_policy in hub_email_policy_identity_pairs
            ]

            detach_users_from_policy(lambda_client, policy_identity_pairs_to_delete)

            conn.commit()


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to delete hub account'
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
        'body': 'Hub Accounts Deleted Successfully'
    }


