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

policy_creation_lambda = "CreatePolicy"
policy_attach_lambda = "AttachPolicy"


def is_in_org(cursor, schema, user_organisations_intersect_table, login_user_id):
    try:
        logging.info("Checking user permissions...")
        sql = f"SELECT DISTINCT userID FROM {database_dict['schema']}.{database_dict['users_organisations_table']} WHERE userid = %s LIMIT 1;"
        cursor.execute(sql, (login_user_id,))
        in_org = cursor.fetchone()
        return in_org is not None
    except Exception as e:
        logging.error(f"Error checking user permissions: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_organisation(cursor, organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number,
                        userUUID):
    try:
        logging.info("Creating Organisation...")

        # Step 1: Create organisation entry in database
        try:
            sql = f"""
                INSERT INTO {database_dict['schema']}.{database_dict['organisations_table']} 
                (organisation_name, associated_policy, address_line_1, address_line_2, city, county, postcode, phone_no) 
                VALUES (%s, CONCAT('Policy_', UUID()), %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql,
                           (organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number))

            sql_audit = sql % (organisation_name, address_line_1, address_line_2, city, county, postcode, phone_number)

            logging.info("Organisation entry created successfully.")
        except Exception as e:
            logging.error(f"Error executing SQL to create organisation entry: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Step 2: Fetch the last inserted ID
        try:
            organisation_id = zanolambdashelper.helpers.get_last_inserted_row(cursor)
            if not organisation_id:
                raise ValueError("No organisation ID returned after insertion.")
            logging.info(f"Organisation created with ID: {organisation_id}")
        except Exception as e:
            logging.error(f"Error fetching the last inserted organisation ID: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Step 3: Create audit log
        try:
            get_inserted_row_sql = f"""SELECT * FROM {database_dict['schema']}.{database_dict['organisations_table']} 
                                       WHERE organisationid = %s LIMIT 1"""
            cursor.execute(get_inserted_row_sql, (organisation_id,))
            last_inserted_row = cursor.fetchone()
            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                row_dict = dict(zip(colnames, last_inserted_row))
                orgUUID = row_dict['organisationUUID']
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
                # Submit to audit log
                try:
                    zanolambdashelper.helpers.submit_to_audit_log(
                        cursor, database_dict['schema'], database_dict['audit_log_table'],
                        database_dict['organisations_table'], 3, organisation_id, sql_audit,
                        '{}', inserted_row_json, orgUUID, userUUID
                    )
                    logging.info("Audit log submitted successfully.")
                except Exception as e:
                    logging.error(f"Error submitting audit log: {e}")
                    traceback.print_exc()
                    raise  # Re-raise to propagate to the outer block
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error fetching inserted row for audit logging: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Return the organisation ID
        return organisation_id, orgUUID

    except Exception as e:
        # Outermost exception handling
        logging.error(f"Unexpected error in create_organisation: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_user_organisation_relation(cursor, login_user_id, organisation_id, org_UUID, userUUID):
    try:
        logging.info("Adding User To New Organisation...")

        # Step 1: Create user_organisations entry in database
        try:
            sql = f"""
                INSERT INTO {database_dict['schema']}.{database_dict['users_organisations_table']} 
                (userid, organisationid, permissionid) 
                VALUES (%s, %s, 1);
            """
            cursor.execute(sql, (login_user_id, organisation_id))

            sql_audit = sql % (login_user_id, organisation_id)
            logging.info("User organisation relation created successfully.")
        except Exception as e:
            logging.error(f"Error executing SQL to create user organisation relation: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                SELECT * FROM {database_dict['schema']}.{database_dict['users_organisations_table']} 
                WHERE userid = %s AND organisationid = %s LIMIT 1
            """
            cursor.execute(get_inserted_row_sql, (login_user_id, organisation_id,))
            last_inserted_row = cursor.fetchone()
            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                # Submit to audit log
                try:
                    zanolambdashelper.helpers.submit_to_audit_log(
                        cursor, database_dict['schema'], database_dict['audit_log_table'],
                        database_dict['users_organisations_table'], 3, organisation_id, sql_audit,
                        '{}', inserted_row_json, org_UUID, userUUID
                    )
                    logging.info("Audit log submitted successfully.")
                except Exception as e:
                    logging.error(f"Error submitting audit log: {e}")
                    traceback.print_exc()
                    raise  # Re-raise to propagate to the outer block
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error fetching inserted row for audit logging: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

    except Exception as e:
        # Outermost exception handling
        logging.error(f"Error creating user organisation relation: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_default_pool(cursor, organisation_id, organisation_name, org_uuid, user_uuid):
    try:
        logging.info("Creating new organisation default pool...")

        # Step 1: Create default pool entry in database
        try:
            sql = f"""
                INSERT INTO {database_dict['schema']}.{database_dict['pools_table']} 
                (organisationid, pool_name, parentid) 
                VALUES (%s, %s, NULL)
            """
            cursor.execute(sql, (organisation_id, f"{organisation_name} Default Pool"))
            sql_audit = sql % (organisation_id, f"{organisation_name} Default Pool")
            logging.info("Default pool entry created successfully.")
        except Exception as e:
            logging.error(f"Error executing SQL to create default pool: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Step 2: Fetch the last inserted ID
        try:
            pool_id = zanolambdashelper.helpers.get_last_inserted_row(cursor)
            if not pool_id:
                raise ValueError("No pool ID returned after insertion.")
            logging.info(f"Default pool created with ID: {pool_id}")
        except Exception as e:
            logging.error(f"Error fetching the last inserted pool ID: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Step 3: Create audit log
        try:
            get_inserted_row_sql = f"""
                SELECT * FROM {database_dict['schema']}.{database_dict['pools_table']} 
                WHERE poolid = %s LIMIT 1
            """
            cursor.execute(get_inserted_row_sql, (pool_id,))
            last_inserted_row = cursor.fetchone()
            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                # Submit to audit log
                try:
                    zanolambdashelper.helpers.submit_to_audit_log(
                        cursor, database_dict['schema'], database_dict['audit_log_table'],
                        database_dict['pools_table'], 3, pool_id, sql_audit,
                        '{}', inserted_row_json, org_uuid, user_uuid
                    )
                    logging.info("Audit log submitted successfully.")
                except Exception as e:
                    logging.error(f"Error submitting audit log: {e}")
                    traceback.print_exc()
                    raise  # Re-raise to propagate to the outer block
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error fetching inserted row for audit logging: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

        # Return the pool ID
        return pool_id

    except Exception as e:
        # Outermost exception handling
        logging.error(f"Error creating default pool entry: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def add_user_to_pool(cursor, pool_id, login_user_id, org_uuid, user_uuid):
    logging.info("Adding user to newly created default pool...")
    try:
        # Step 1: Create pools_users entry in database
        sql = f"""
            INSERT INTO {database_dict['schema']}.{database_dict['pools_users_table']} 
            (poolid, userid) 
            VALUES (%s, %s);
        """
        cursor.execute(sql, (pool_id, login_user_id))
        sql_audit = sql % (pool_id, login_user_id)
        logging.info("User added to pool successfully.")

        # Step 2: Create audit log
        try:
            get_inserted_row_sql = f"""
                SELECT * FROM {database_dict['schema']}.{database_dict['pools_users_table']} 
                WHERE poolid = %s AND userid = %s LIMIT 1
            """
            cursor.execute(get_inserted_row_sql, (pool_id, login_user_id))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                inserted_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)

                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['pools_users_table'], 3, pool_id, sql_audit,
                    '{}', inserted_row_json, org_uuid, user_uuid
                )
                logging.info("Audit log submitted successfully.")
            else:
                logging.error("No row found after insertion for audit logs.")
                raise ValueError("Inserted row not found for audit log.")
        except Exception as e:
            logging.error(f"Error creating audit log: {e}")
            traceback.print_exc()
            raise  # Re-raise to propagate to the outer block

    except Exception as e:
        logging.error(f"Error adding user to pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def update_user_identity_pool(cursor, user_identity, login_user_id, org_uuid, user_uuid):
    logging.info("Setting user's identity_pool_id...")
    try:

        get_previous_sql = f"""
                SELECT * FROM {database_dict['schema']}.{database_dict['users_table']} 
                WHERE userID = %s LIMIT 1
            """
        cursor.execute(get_previous_sql, (login_user_id,))
        historic_row = cursor.fetchone()

        if historic_row:
            colnames = [desc[0] for desc in cursor.description]
            historic_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, historic_row)
        else:
            logging.error("No row found before update for audit logs.")
            raise ValueError("Inital row not found for audit log.")

        # Step 1: Update the user entry to include the identity pool ID
        sql = f"""
            UPDATE {database_dict['schema']}.{database_dict['users_table']} 
            SET identity_pool_id = %s 
            WHERE userID = %s
        """
        cursor.execute(sql, (user_identity, login_user_id))
        sql_audit = sql % (user_identity, login_user_id)
        logging.info("User identity pool updated successfully.")

        # Step 2: Create audit log
        try:
            get_updated_row_sql = f"""
                SELECT * FROM {database_dict['schema']}.{database_dict['users_table']} 
                WHERE userID = %s LIMIT 1
            """
            cursor.execute(get_updated_row_sql, (login_user_id,))
            last_inserted_row = cursor.fetchone()

            if last_inserted_row:
                colnames = [desc[0] for desc in cursor.description]
                updated_row_json = zanolambdashelper.helpers.convert_col_to_json(colnames, last_inserted_row)
                zanolambdashelper.helpers.submit_to_audit_log(
                    cursor, database_dict['schema'], database_dict['audit_log_table'],
                    database_dict['users_table'], 1, login_user_id, sql_audit,
                    historic_row_json, updated_row_json, org_uuid, user_uuid
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
        logging.error(f"Error updating user identity pool: {e}")
        traceback.print_exc()
        raise Exception(400, e)


def create_and_attach_policy(cursor, organisation_id, organisation_name, policy_creation_lambda, policy_attach_lambda,
                             user_identity):
    try:
        logging.info("Creating and Attatching IoT policy to organisation...")
        # Fetch associated policy and organisation UUID
        sql = f"SELECT associated_policy, organisationUUID FROM {database_dict['organisations_table']} WHERE organisationid = %s;"
        cursor.execute(sql, (organisation_id,))
        result = cursor.fetchone()
        policy_name = result[0]
        organisation_uuid = result[1]

        # Run policy creation lambda
        response = lambda_client.invoke(
            FunctionName=policy_creation_lambda,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps({"policy_name": policy_name, "organisation_UUID": organisation_uuid})
        )
        logging.info("Policy created")

        response_payload = response['Payload'].read().decode('utf-8')
        logging.info(response_payload)

        if response['StatusCode'] != 200 or 'errorMessage' in response_payload:
            logging.error(f"Lambda invocation failed, ResponsePayload: {response_payload}")
            traceback.print_exc()
            raise Exception(400, {response_payload})

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
            raise Exception(400, e)

    except Exception as e:
        logging.error(f"Error creating and attaching policy: {e}")
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
        organisation_name_raw = body_json.get('organisation_name')
        address_line_1_raw = body_json.get('address_line_1')
        address_line_2_raw = body_json.get('address_line_2', None)
        city_raw = body_json.get('city')
        county_raw = body_json.get('county')
        postcode_raw = body_json.get('postcode')
        phone_number_raw = body_json.get('phone_number')
        user_identity_raw = body_json.get('user_identity')

        variables = {
            'organisation_name': {'value': organisation_name_raw['value'],
                                  'value_type': organisation_name_raw['value_type']},
            'address_line_1': {'value': address_line_1_raw['value'], 'value_type': address_line_1_raw['value_type']},
            'city': {'value': city_raw['value'], 'value_type': city_raw['value_type']},
            'county': {'value': county_raw['value'], 'value_type': county_raw['value_type']},
            'postcode': {'value': postcode_raw['value'], 'value_type': postcode_raw['value_type']},
            'phone_number': {'value': phone_number_raw['value'], 'value_type': phone_number_raw['value_type']},
            'user_identity': {'value': user_identity_raw['value'], 'value_type': user_identity_raw['value_type']}
        }

        if address_line_2_raw['value']:  # add optionals if exists
            variables['address_line_2'] = {'value': address_line_2_raw['value'],
                                           'value_type': address_line_2_raw['value_type']}

        logging.info("Validating and cleansing user inputs...")
        variables = zanolambdashelper.helpers.validate_and_cleanse_values(variables)

        organisation_name = variables['organisation_name']['value']
        address_line_1 = variables['address_line_1']['value']
        address_line_2 = variables['address_line_2']['value'] if address_line_2_raw['value'] else None
        city = variables['city']['value']
        county = variables['county']['value']
        postcode = variables['postcode']['value']
        phone_number = variables['phone_number']['value']
        user_identity = variables['user_identity']['value']

        with conn.cursor() as cursor:
            login_user_id, userUUID = zanolambdashelper.helpers.get_user_details_by_email(cursor,
                                                                                          database_dict['schema'],
                                                                                          database_dict['users_table'],
                                                                                          user_email)

            if not is_in_org(cursor, database_dict['schema'], database_dict['users_organisations_table'],
                             login_user_id):

                # Create organisation entry in database
                organisation_id, org_uuid = create_organisation(cursor, organisation_name, address_line_1,
                                                                address_line_2, city, county, postcode, phone_number,
                                                                userUUID)

                # Create user_organisations entry in database
                create_user_organisation_relation(cursor, login_user_id, organisation_id, org_uuid, userUUID)

                # Create default pool entry in database
                pool_id = create_default_pool(cursor, organisation_id, organisation_name, org_uuid, userUUID)

                # Add user to pool
                add_user_to_pool(cursor, pool_id, login_user_id, org_uuid, userUUID)

                # Update user identity pool
                update_user_identity_pool(cursor, user_identity, login_user_id, org_uuid, userUUID)

                # Create and attach policy
                create_and_attach_policy(cursor, organisation_id, organisation_name, policy_creation_lambda,
                                         policy_attach_lambda, user_identity)

                conn.commit()

            else:
                traceback.print_exc()
                raise Exception(400)


    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        status_value = e.args[0]
        if status_value == 422:  # if 422 then validation
            body_value = e.args[1]
        else:
            body_value = 'Unable to create organisation'
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
        'body': 'Organisation Created Successfully'
    }

