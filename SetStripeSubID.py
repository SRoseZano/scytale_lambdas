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

stripe_secrets = zanolambdashelper.helpers.get_stripe_webhook_secrets()

STRIPE_WEBHOOK_SECRET = stripe_secrets['webhook_secret']

database_dict = zanolambdashelper.helpers.get_database_dict()

rds_client = zanolambdashelper.helpers.create_client('rds')

zanolambdashelper.helpers.set_logging('INFO')


def update_org_stripe_sub_id(cursor, org_uuid, sub_id):
    logging.info("Setting org subID...")
    try:

        get_previous_sql = f"""
                SELECT * FROM {database_dict['schema']}.{database_dict['organisations_table']} 
                WHERE organisationUUID = %s LIMIT 1
            """
        cursor.execute(get_previous_sql, (org_uuid,))
        historic_row = cursor.fetchone()

        if historic_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, historic_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        # Step 1: Update the org entry to include the stripe sub ID
        sql = f"""
            UPDATE {database_dict['schema']}.{database_dict['organisations_table']} 
            SET stripe_sub_id = %s 
            WHERE organisationUUID = %s
        """
        cursor.execute(sql, (sub_id, org_uuid))
        sql_audit = sql % (sub_id, org_uuid)
        logging.info("Org table updated successfully.")

        # Step 2: Create audit log
        try:
            get_updated_row_sql = f"""
                 SELECT * FROM {database_dict['schema']}.{database_dict['organisations_table']} 
                WHERE organisationUUID = %s LIMIT 1
            """
            cursor.execute(get_updated_row_sql, (org_uuid,))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                updated_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['users_table'], 1, org_uuid, sql_audit,
                    historic_row_json, updated_row_json, org_uuid, None
                )
                logging.info("Audit log submitted successfully.")
            else:
                logging.error("No row found after update for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error creating audit log: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

    except Exception as e:
        logging.error(f"Error updating organisation stripe sub id: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def lambda_handler(event, context):
    try:

        if event.get("isBase64Encoded"):
            raw_body = base64.b64decode(event["body"]).decode("utf-8")
        else:
            raw_body = event["body"]

        # signature header (case-insensitive)
        sig_header = (
                event["headers"].get("Stripe-Signature")
                or event["headers"].get("stripe-signature")
        )

        try:
            stripe_event = stripe.Webhook.construct_event(
                payload=raw_body,
                sig_header=sig_header,
                secret=STRIPE_WEBHOOK_SECRET
            )
        except stripe.error.SignatureVerificationError as e:
            logging.error(e)
            raise Exception(400, f"{e}")

        # Process only relevant events
        if stripe_event["type"] in ["checkout.session.completed", "customer.subscription.created"]:
            data_object = stripe_event["data"]["object"]

            # If checkout session, get subscription from it
            if stripe_event["type"] == "checkout.session.completed":
                sub_id = data_object.get("subscription")
                org_uuid = data_object.get("metadata", {}).get("org_uuid")
            else:
                # Direct subscription event
                sub_id = data_object.get("id")
                org_uuid = data_object.get("metadata", {}).get("org_uuid")

            if not sub_id or not org_uuid:
                logging.error("Missing sub_id or org_uuid in webhook payload.")
                raise Exception(400, "Missing sub_id or org_uuid in webhook payload.")

        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        with conn.cursor() as cursor:

            update_org_stripe_sub_id(cursor, org_uuid, sub_id)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to update org subscription'
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
        'body': 'Subscription Added Successfully'
    }

