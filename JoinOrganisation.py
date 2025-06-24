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

policy_attach_lambda = "AttachPolicy"


def get_user_and_hub_id_by_email(cursor,
                                 user_email):  # not using helper get id function because this one also requires hubid for join logic
    try:
        logging.info("Getting user details...")
        sql = f"SELECT hub_user, userUUID FROM {database_dict['schema']}.{database_dict['users_table']} WHERE email = %s"
        cursor.execute(sql, (user_email,))
        result = cursor.fetchone()
        if result:
            return result
        else:
            raise ValueError("UserUUID doesn't exist for provided user email")
    except Exception as e:
        logging.error(f"Error fetching user ID by email: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def join_organisation(cursor, invite_code, login_user_hub, user_uuid):
    try:
        logging.info("Joining Organisation...")
        if invite_code == 1:
            get_organisation_uuid_sql = f""" SELECT DISTINCT organisationUUID, inviteID FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE invite_code = %s AND valid_until >= NOW() LIMIT 1 """
        else:
            get_organisation_uuid_sql = f""" SELECT DISTINCT organisationUUID, inviteID FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE invite_code = %s LIMIT 1 """
        cursor.execute(get_organisation_uuid_sql, (invite_code,))
        get_organisation_uuid_sql_result = cursor.fetchone()
        if get_organisation_uuid_sql_result:
            logging.info("OrganisationUUID found")
            if get_organisation_uuid_sql_result[1] == 3 and login_user_hub == 1:  # if invite type is hub and the
                join_organisation_sql = f""" INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userUUID, organisationUUID, permissionid) VALUES (%s, %s, 2);"""
            else:
                join_organisation_sql = f""" INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userUUID, organisationUUID, permissionid) VALUES (%s, %s, 3);"""
            cursor.execute(join_organisation_sql, (user_uuid, get_organisation_uuid_sql_result[0]))
            logging.info("User organisation relation created")

            get_inserted_row_sql = f"""
                            SELECT * FROM {database_dict['schema']}.{database_dict['users_organisations_table']}
                            WHERE organisationUUID = %s AND userUUID = %s LIMIT 1
                        """
            cursor.execute(get_inserted_row_sql, (get_organisation_uuid_sql_result[0], user_uuid))
            last_inserted_row = cursor.fetchone()

            get_org_uuid_sql = f"""
                            SELECT organisationUUID FROM {database_dict['schema']}.{database_dict['organisations_table']}
                            WHERE organisationUUID = %s LIMIT 1
                        """
            cursor.execute(get_org_uuid_sql, (get_organisation_uuid_sql_result[0],))
            org_uuid = cursor.fetchone()[0]

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
                row_dict = dict(zip(colnames, last_inserted_row))

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['users_organisations_table'], 3, user_uuid, join_organisation_sql,
                    '{}', inserted_row_json, org_uuid, user_uuid
                )
                logging.info("Audit log submitted successfully.")
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")

            return get_organisation_uuid_sql_result, org_uuid
        else:
            logging.error(f"Invite Code Invalid")
            traceback.print_exc()
            raise Exception(400)
    except Exception as e:
        logging.error(f"Error joining organisation: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def configure_mqtt(cursor, user_identity,  org_uuid, user_uuid):
    try:
        logging.info("Configuring org policy to user identity...")

        update_user_identity_pool(cursor, user_identity, org_uuid, user_uuid)
        attach_policy(cursor, org_uuid, user_identity)
    except Exception as e:
        logging.error(f"Error configuring mqtt: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def update_user_identity_pool(cursor, user_identity, org_uuid, user_uuid):
    logging.info("Setting users identity_pool_id...")
    try:

        get_entry = f"""
                                            SELECT * FROM {database_dict['schema']}.{database_dict['users_table']}
                                            WHERE userUUID = %s LIMIT 1
                                        """
        cursor.execute(get_entry, (user_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        # Update the user entry to include the identity pool ID
        sql = f"UPDATE {database_dict['schema']}.{database_dict['users_table']} SET identity_pool_id = %s WHERE userUUID = %s"
        cursor.execute(sql, (user_identity, user_uuid))

        sql_audit = sql % (user_identity, user_uuid)

        logging.info("User identity pool updated")

        cursor.execute(get_entry, (user_uuid,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['users_table'], 1, user_uuid, sql_audit,
            historic_row_json, current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error updating user identity pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def attach_policy(cursor, org_uuid, user_identity):
    try:
        logging.info("Attatching IoT policy to user identity...")
        # Fetch associated policy and organisation UUID
        sql = f"SELECT associated_policy FROM {database_dict['organisations_table']} WHERE organisationUUID = %s;"
        cursor.execute(sql, (org_uuid,))
        result = cursor.fetchone()
        policy_name = result[0]

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
            raise Exception(400, response_payload)

    except Exception as e:
        logging.error(f"Error attaching policy: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def append_user_to_all_pools(cursor, org_uuid, user_uuid):
    try:
        logging.info("Executing SQL query to append user to all org pools...")
        # SQL query to find top level pool and assign to everyone under it
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userUUID, poolUUID)
            WITH RECURSIVE PoolHierarchy AS (
                SELECT parentUUID, poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_table']}
                WHERE parentUUID is NULL AND organisationUUID = %s

                UNION

                SELECT p.parentUUID, p.poolUUID
                FROM {database_dict['schema']}.{database_dict['pools_table']} p
                JOIN PoolHierarchy ph ON ph.poolUUID = p.parentUUID

            )
            SELECT %s AS userUUID, poolUUID
            FROM PoolHierarchy ph
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM {database_dict['schema']}.{database_dict['pools_users_table']} dp
                    WHERE dp.userUUID = %s
                    AND dp.poolUUID = ph.poolUUID
                );

        """

        cursor.execute(sql, (org_uuid, user_uuid, user_uuid))

        sql_audit = sql % (org_uuid, user_uuid, user_uuid)

        get_entry = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']}
                        WHERE userUUID = %s
                    """
        cursor.execute(get_entry, (user_uuid,))
        last_inserted_row = cursor.fetchall()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['users_organisations_table'], 1, user_uuid, sql_audit,
            '{}', current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def append_user_to_default_pool(cursor, org_uuid, user_uuid):
    try:
        logging.info("Executing SQL query to append user to all org pools...")
        # SQL query to find top level pool and assign to everyone under it
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userUUID, poolUUID)
            SELECT %s AS userUUID, poolUUID
            FROM {database_dict['schema']}.{database_dict['pools_table']}
            WHERE parentUUID IS NULL AND organisationUUID = %s;

        """

        cursor.execute(sql, (user_uuid, org_uuid,))

        get_entry = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']} a 
                        JOIN {database_dict['schema']}.{database_dict['pools_table']} b 
                        ON a.poolUUID = b.poolUUID AND b.parentUUID is NULL AND b.organisationUUID = %s AND a.userUUID = %s
                        LIMIT 1
        """
        cursor.execute(get_entry, (org_uuid, user_uuid,))

        sql_audit = sql % (org_uuid, user_uuid,)

        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['pools_users_table'], 3, user_uuid, sql_audit,
            '{}', current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
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

        # Extract relevant attributes if non existant set empty
        invite_code_raw = body_json.get('invite_code')
        user_identity_raw = body_json.get('user_identity')

        variables = {
            'invite_code': {'value': invite_code_raw['value'], 'value_type': 'string_input'},
            'user_identity': {'value': user_identity_raw['value'], 'value_type': 'string_input'},
        }

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        invite_code = variables['invite_code']['value']
        user_identity = variables['user_identity']['value']

        with conn.cursor() as cursor:

            login_user_hub, user_uuid = get_user_and_hub_id_by_email(cursor, user_email)

            org_invite_details, org_uuid = join_organisation(cursor, invite_code, login_user_hub,
                                                             user_uuid)

            if (login_user_hub == 1):  # if new user is hub add user to all pools (for hub get org details )
                append_user_to_all_pools(cursor, org_uuid, user_uuid)
            else:
                append_user_to_default_pool(cursor, org_uuid, user_uuid)

            configure_mqtt(cursor, user_identity,  org_uuid, user_uuid)

            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable join organisation'
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
        'body': 'Joined Organisation Successfully',

    }
