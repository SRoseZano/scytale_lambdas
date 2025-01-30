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


policy_attach_lambda = "AttachPolicy"

def get_user_and_hub_id_by_email(cursor, user_email): #not using helper get id function because this one also requires hubid for join logic
    try:
        logging.info("Getting user details...")
        sql = f"SELECT userid, hub_user, userUUID FROM {database_dict['schema']}.{database_dict['users_table']} WHERE email = %s"
        cursor.execute(sql, (user_email,))
        result = cursor.fetchone()
        if result:
            return result
        else:
            raise ValueError("UserID doesn't exist for provided user email")
    except Exception as e:
        logging.error(f"Error fetching user ID by email: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def join_organisation(cursor,invite_code, login_user_id, login_user_hub, user_uuid):
    try:
        logging.info("Joining Organisation...")
        if invite_code == 1:
            get_organisationid_sql = f""" SELECT DISTINCT organisationID, inviteID FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE invite_code = %s AND valid_until >= NOW() LIMIT 1 """
        else:
             get_organisationid_sql = f""" SELECT DISTINCT organisationID, inviteID FROM {database_dict['schema']}.{database_dict['organisation_invites_table']} WHERE invite_code = %s LIMIT 1 """
        cursor.execute(get_organisationid_sql, (invite_code,))
        get_organisationid_sql_result = cursor.fetchone()
        if get_organisationid_sql_result:
            logging.info("OrganisationID found")
            if get_organisationid_sql_result[1] == 3 and login_user_hub == 1: #if invite type is hub and the
                join_organisation_sql = f""" INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userid, organisationid, permissionid) VALUES (%s, %s, 2);"""
            else:
                join_organisation_sql = f""" INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} (userid, organisationid, permissionid) VALUES (%s, %s, 3);"""
            cursor.execute(join_organisation_sql, (login_user_id, get_organisationid_sql_result[0]))
            logging.info("User organisation relation created")

            get_inserted_row_sql = f"""
                            SELECT * FROM {database_dict['schema']}.{database_dict['users_organisations_table']} 
                            WHERE organisationID = %s AND userid = %s LIMIT 1
                        """
            cursor.execute(get_inserted_row_sql, (get_organisationid_sql_result[0], login_user_id))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
                row_dict = dict(zip(col_names, last_inserted_row))
                orgUUID = row_dict['organisationUUID']

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['users_organisations_table'], 3, login_user_id, join_organisation_sql,
                    '{}', inserted_row_json, org_uuid, user_uuid
                )
                logging.info("Audit log submitted successfully.")
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")

            return get_organisationid_sql_result, orgUUID
        else:
            logging.error(f"Invite Code Invalid")
            traceback.print_exc()
            raise Exception(400)
    except Exception as e:
        logging.error(f"Error joining organisation: {e}")
        traceback.print_exc()
        raise Exception(400, e)
        
def configure_mqtt(cursor,login_user_id, user_identity, organisation_id):
    try:
        logging.info("Configuring org policy to user identity...")
       
        update_user_identity_pool(cursor,user_identity,login_user_id)
        attach_policy(cursor,organisation_id,user_identity)
    except Exception as e:
        logging.error(f"Error configuring mqtt: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def update_user_identity_pool(cursor, user_identity, login_user_id, org_uuid, user_uuid):
    logging.info("Setting users identity_pool_id...")
    try:

        get_entry = f"""
                                            SELECT * FROM {database_dict['schema']}.{database_dict['users_table']}
                                            WHERE userID = %s LIMIT 1
                                        """
        cursor.execute(get_entry, (user_id,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        # Update the user entry to include the identity pool ID
        sql = f"UPDATE {database_dict['schema']}.{database_dict['users_table']} SET identity_pool_id = %s WHERE userID = %s"
        cursor.execute(sql, (user_identity, login_user_id))
        logging.info("User identity pool updated")

        cursor.execute(get_entry, (user_id,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")


        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['users_table'], 1, user_id, sql,
            historic_row_json, current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")

    except Exception as e:
        logging.error(f"Error updating user identity pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def attach_policy(cursor, organisation_id, user_identity):
    try:
        logging.info("Attatching IoT policy to user identity...")
        # Fetch associated policy and organisation UUID
        sql = f"SELECT associated_policy FROM {database_dict['organisations_table']} WHERE organisationid = %s;"
        cursor.execute(sql, (organisation_id,))
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
        raise Exception(400,e)
        


def append_user_to_all_pools(cursor, organisation_id, user_id, org_uuid, user_uuid):
    try:
        logging.info("Executing SQL query to append user to all org pools...")
        # SQL query to find top level pool and assign to everyone under it
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userid, poolid)
            WITH RECURSIVE PoolHierarchy AS (
                SELECT parentid, poolID
                FROM {database_dict['schema']}.{database_dict['pools_table']}
                WHERE parentID is NULL AND organisationid = %s
    
                UNION
    
                SELECT p.parentid, p.poolID
                FROM {database_dict['schema']}.{database_dict['pools_table']} p
                JOIN PoolHierarchy ph ON ph.poolID = p.parentID
            
            )
            SELECT %s AS userid, poolID
            FROM PoolHierarchy ph
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM {database_dict['schema']}.{database_dict['pools_users_table']} dp
                    WHERE dp.userid = %s
                    AND dp.poolid = ph.poolID
                );

        """
        
        cursor.execute(sql, (organisation_id, user_id, user_id))

        get_entry = f"""
                                                 SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']}
                                                 WHERE userID = %s LIMIT 1
                                             """
        cursor.execute(get_entry, (user_id,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['users_organisations_table'], 1, user_id, sql,
            '{}', current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")
    
    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def append_user_to_default_pool(cursor, organisation_id, user_id, org_uuid, user_uuid):
    try:
        logging.info("Executing SQL query to append user to all org pools...")
        # SQL query to find top level pool and assign to everyone under it
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} (userid, poolid)
            SELECT %s AS userid, poolid
            FROM {database_dict['schema']}.{database_dict['pools_table']}
            WHERE parentid IS NULL AND organisationid = %s;

        """
        
        cursor.execute(sql, (user_id,organisation_id,))

        get_entry = f"""
                        SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']} a 
                        JOIN {database_dict['schema']}.{database_dict['pools_table']} b 
                        ON a.poolid = b.poolid AND b.parentid is NULL AND b.organisationid = %s
                        LIMIT 1
        """
        cursor.execute(get_entry, (organisation_id, user_id,))
        last_inserted_row = cursor.fetchone()
        if last_inserted_row:
            colnames = [desc[0] for desc in cursor.description]
            current_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        zanolambdashelper.helpers.submit_to_audit_log(
            cursor, database_dict['schema'], database_dict['audit_log_table'],
            database_dict['pools_users_table'], 3, user_id, sql,
            '{}', current_row_json, org_uuid, user_uuid
        )
        logging.info("Audit log submitted successfully.")
    
    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
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
        invite_code_raw = body_json.get('invite_code')
        user_identity_raw = body_json.get('user_identity')
        
        variables = {
            'invite_code': {'value': invite_code_raw['value'], 'value_type': invite_code_raw['value_type']},
            'user_identity': {'value': user_identity_raw['value'], 'value_type': user_identity_raw['value_type']},
        }
        
        logging.info("Validating and cleansing user inputs...")
        variables =  zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        invite_code = variables['invite_code']['value']
        user_identity = variables['user_identity']['value']
        
        with conn.cursor() as cursor:

            login_user_id, login_user_hub, user_uuid = get_user_and_hub_id_by_email(cursor, user_email)

            org_invite_details, org_uuid = join_organisation(cursor,invite_code,login_user_id, login_user_hub)
            
            if(login_user_hub == 1): #if new user is hub add user to all pools (for hub get org details )
                append_user_to_all_pools(cursor,org_invite_details[0],login_user_id, org_uuid, user_uuid)
            else:
                append_user_to_default_pool(cursor,org_invite_details[0],login_user_id, org_uuid,user_uuid)
            
            configure_mqtt(cursor,login_user_id, user_identity, org_invite_details[0])

            conn.commit()
           
    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation 
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
