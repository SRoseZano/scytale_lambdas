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

policy_deletion_lambda = "DeletePolicy"
policy_detatch_lambda = "DetachPolicy"



def get_user_identities(cursor, organisation_id):
    try:
        logging.info("Fetching user identities...")
        sql = f"""
            SELECT DISTINCT a.identity_pool_id 
            FROM {database_dict['schema']}.{database_dict['users_table']} a
            INNER JOIN {database_dict['users_organisations_table']} b ON a.userid = b.userid 
            AND organisationid = %s
        """
        cursor.execute(sql, (organisation_id,))
        user_identities = cursor.fetchall()
        user_identities = [identity[0] for identity in user_identities]
        return user_identities
    except Exception as e:
        logging.error(f"Error fetching user identities: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def get_associated_policy(cursor, organisation_id):
    try:
        logging.info("Fetching associated policy...")
        sql = f"SELECT associated_policy FROM {database_dict['schema']}.{database_dict['organisations_table']} WHERE organisationid = %s;"
        cursor.execute(sql, (organisation_id,))
        policy_name = cursor.fetchone()[0]
        return policy_name
    except Exception as e:
        logging.error(f"Error fetching associated policy: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def delete_organisation(cursor, organisation_id):
    try:
        logging.info("Deleting organisation...")
        sql = f"DELETE FROM {database_dict['schema']}.{database_dict['organisations_table']} WHERE organisationid = %s"
        cursor.execute(sql, (organisation_id,))
        logging.info("Organisation deleted successfully.")
    
    except Exception as e:
        logging.error(f"Error deleting organisation: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def detach_users_from_policy(lambda_client, policy_detatch_lambda, policy_name, user_identities):
    try:
        print(user_identities)
        for user_identity in user_identities:
            print(user_identity)
            response = lambda_client.invoke(
                FunctionName=policy_detatch_lambda,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=json.dumps({"policy_name": policy_name, "user_identity": user_identity})
            )

            response_payload = response['Payload'].read().decode('utf-8')

            if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
                logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
                traceback.print_exc()
                raise Exception(400, e)
                
    except Exception as e:
        logging.error(f"Error detaching users from policy: {e}")
        traceback.print_exc()
        raise Exception(400, e)

def delete_associated_policy(lambda_client, policy_deletion_lambda, policy_name):
    try:
        response = lambda_client.invoke(
            FunctionName=policy_deletion_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps({"policy_name": policy_name})
        )

        response_payload = response['Payload'].read().decode('utf-8')

        if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
            logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
            traceback.print_exc()
            raise Exception(400, e)
    except Exception as e:
        logging.error(f"Error deleting associated policy: {e}")
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

        with conn.cursor() as cursor:
            
            login_user_id = zanolambdashelper.helpers.get_user_id_by_email(cursor, database_dict['schema'], database_dict['users_table'], user_email)
            organisation_id = zanolambdashelper.helpers.get_user_organisation(cursor, database_dict['schema'],database_dict['users_organisations_table'], login_user_id)
            
            #validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_admin(cursor,database_dict['schema'], database_dict['users_organisations_table'], login_user_id, organisation_id)

            user_identities = get_user_identities(cursor,  organisation_id)
            policy_name = get_associated_policy(cursor,organisation_id)
            delete_organisation(cursor, organisation_id)
            detach_users_from_policy(lambda_client, policy_detatch_lambda, policy_name, user_identities)
            delete_associated_policy(lambda_client, policy_deletion_lambda, policy_name)

            conn.commit()

            
    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422: # if 422 then validation error
            body_value = e.args[1]
        else:
            body_value = 'Unable to delete organisation'
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
        'body': 'Organisation Removed Successfully'
    }
