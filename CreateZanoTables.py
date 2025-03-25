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

# Sample dictionary with values
status_lookup_data = [{
    "status_code": 200,
    "status_message": "OK",
    "status_type_id": 1
},
    {
        "status_code": 201,
        "status_message": "WARNING",
        "status_type_id": 2
    },
    {
        "status_code": 400,
        "status_message": "ERROR",
        "status_type_id": 3
    }
]


def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        with conn.cursor() as cursor:

            # Drop all tables if they exist
            drop_tables = """
                DROP TABLE IF EXISTS users, 
                organisations, 
                pools, 
                permissions_lookup, 
                users_organisations, 
                pools_users, 
                devices, 
                hubs,
                pools_devices, 
                organisation_invites, 
                invite_lookup,
                device_lookup,
                audit_log,
                audit_operation_lookup,
                warning_lookup,
                status_type_lookup,
                status_lookup

            """
            cursor.execute(drop_tables)

            create_audit_operation_type_lookup_table = """
                CREATE TABLE audit_operation_lookup (
                        operationID INT PRIMARY KEY,
                        operation_name VARCHAR(100) NOT NULL
                );
            """

            cursor.execute(create_audit_operation_type_lookup_table)

            create_audit_log_table = """
                CREATE TABLE audit_log (
                    audit_id SERIAL PRIMARY KEY, 
                    table_name VARCHAR(100),    
                    operation_type INT,  
                    record_id INT,               
                    sql_query TEXT,               
                    old_payload JSON,             
                    new_payload JSON,              
                    organisationUUID VARCHAR(36), 
                    changed_by_userUUID VARCHAR(36),   
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,
                    FOREIGN KEY (operation_type) REFERENCES audit_operation_lookup(operationID),
                    INDEX (organisationUUID),
                    INDEX (changed_by_userUUID)
                );
            """

            cursor.execute(create_audit_log_table)

            # Create Users table
            create_users_table = """
                CREATE TABLE users (
                userUUID VARCHAR(36) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                birthdate DATE NOT NULL,
                identity_pool_id VARCHAR(255) DEFAULT NULL,
                zone_info VARCHAR(100) NOT NULL,
                locale VARCHAR(20) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hub_user BOOL NOT NULL DEFAULT 0,
                hubUUID VARCHAR(36), 
                PRIMARY KEY (userUUID),
                INDEX (email)
            );
            """
            cursor.execute(create_users_table)

            # Create organisation table
            create_organisation_table = """
            CREATE TABLE organisations (
                organisationUUID VARCHAR(36) NOT NULL,
                organisation_name VARCHAR(255) NOT NULL,
                associated_policy VARCHAR(255) NOT NULL,
                address_line_1 VARCHAR(255) NOT NULL,
                address_line_2 VARCHAR(255),
                city VARCHAR(50) NOT NULL,
                county VARCHAR(50) NOT NULL,
                postcode VARCHAR(20) NOT NULL,
                phone_no VARCHAR(15) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (organisationUUID),
            );
            """
            cursor.execute(create_organisation_table)

            # Create Pools table
            create_pools_table = """
            CREATE TABLE pools (
                poolUUID VARCHAR(36) NOT NULL,
                organisationUUID INT NOT NULL,
                pool_name VARCHAR(100) NOT NULL,
                parentUUID VARCHAR(36),
                PRIMARY KEY (poolUUID),
                FOREIGN KEY (organisationUUID) REFERENCES organisations(organisationUUID) ON DELETE CASCADE
            );
            """
            cursor.execute(create_pools_table)

            # Create Permissions_Lookup table
            create_device_type_lookup_table = """
            CREATE TABLE device_lookup (
                device_type_ID INT PRIMARY KEY,
                type VARCHAR(100) NOT NULL
            );
            """

            cursor.execute(create_device_type_lookup_table)

            # Create Permissions_Lookup table
            create_permissions_lookup_table = """
            CREATE TABLE permissions_lookup (
                permissionID INT PRIMARY KEY,
                role VARCHAR(100) NOT NULL
            );
            """
            cursor.execute(create_permissions_lookup_table)

            # create invite type lookup table
            create_invite_lookup_table = """
            CREATE TABLE invite_lookup (
                inviteID INT PRIMARY KEY,
                type VARCHAR(100) NOT NULL
            );
            """
            cursor.execute(create_invite_lookup_table)

            # Create org invite table
            create_organisation_invite_table = """
            CREATE TABLE organisation_invites (
                invite_code VARCHAR(6) PRIMARY KEY,
                organisationID INT NOT NULL,
                target_email VARCHAR(255),
                inviteID INT NOT NULL,
                valid_until TIMESTAMP,
                FOREIGN KEY (organisationID) REFERENCES organisations(organisationID) ON DELETE CASCADE,
                FOREIGN KEY (inviteID) REFERENCES invite_lookup(inviteID) ON DELETE CASCADE
            )
            """

            cursor.execute(create_organisation_invite_table)

            # Create Users_organisations table
            create_users_organisations_table = """
            CREATE TABLE users_organisations (
                userUUID VARCHAR(36) NOT NULL,
                organisationUUID VARCHAR(36) NOT NULL,
                permissionID INT NOT NULL,
                PRIMARY KEY (userUUID, organisationUUID),
                FOREIGN KEY (userUUID) REFERENCES users(userUUID) ON DELETE CASCADE,
                FOREIGN KEY (organisationUUID) REFERENCES organisations(organisationUUID) ON DELETE CASCADE,
                FOREIGN KEY (permissionID) REFERENCES permissions_lookup(permissionID) ON DELETE CASCADE
            );
            """
            cursor.execute(create_users_organisations_table)

            # Create Pools_Users table
            create_pools_users_table = """
            CREATE TABLE pools_users (
                poolUUID VARCHAR(36) NOT NULL,
                userUUID VARCHAR(36) NOT NULL,
                PRIMARY KEY (poolUUID, userUUID),
                FOREIGN KEY (poolUUID) REFERENCES pools(poolUUID) ON DELETE CASCADE,
                FOREIGN KEY (userUUID) REFERENCES users(userUUID) ON DELETE CASCADE
            );
            """
            cursor.execute(create_pools_users_table)

            # Create Hubs table
            create_hubs_table = """
            CREATE TABLE hubs (
                hubUUID VARCHAR(36) NOT NULL,
                serial VARCHAR(64) NOT NULL,
                registrant VARCHAR(255) NOT NULL,
                hub_name VARCHAR(255) NOT NULL,
                organisationUUID VARCHAR(36) NOT NULL,
                device_type_ID INT NOT NULL,
                current_firmware VARCHAR(36) NOT NULL,
                target_firmware VARCHAR(36),
                PRIMARY KEY (hubUUID),
                UNIQUE (serial),
                FOREIGN KEY (organisationUUID) REFERENCES organisations(organisationUUID) ON DELETE CASCADE,
                FOREIGN KEY (device_type_ID) REFERENCES device_lookup(device_type_ID) ON DELETE CASCADE
            );

            """
            cursor.execute(create_hubs_table)

            # Create Devices table
            create_devices_table = """
            CREATE TABLE devices (
                deviceUUID VARCHAR(36) NOT NULL,
                long_address VARCHAR(16) NOT NULL,
                short_address VARCHAR(4) NOT NULL,
                associated_hub VARCHAR(64) NOT NULL,
                registrant VARCHAR(255) NOT NULL,
                device_name VARCHAR(255) NOT NULL,
                organisationUUID VARCHAR(36) NOT NULL,
                device_type_ID INT NOT NULL,
                PRIMARY KEY (deviceUUID),
                UNIQUE (long_address),
                UNIQUE (short_address),
                FOREIGN KEY (organisationUUID) REFERENCES organisations(organisationUUID) ON DELETE CASCADE,
                FOREIGN KEY (device_type_ID) REFERENCES device_lookup(device_type_ID) ON DELETE CASCADE,
                FOREIGN KEY (associated_hub) REFERENCES hubs(serial) ON DELETE CASCADE
            );

            """
            cursor.execute(create_devices_table)

            # Create Pools_Devices table
            create_pools_devices_table = """
            CREATE TABLE pools_devices (
                poolUUID VARCHAR(36) NOT NULL,
                deviceUUID VARCHAR(36) NOT NULL,
                PRIMARY KEY (poolUUID, deviceUUID),
                FOREIGN KEY (poolUUID) REFERENCES pools(poolUUID) ON DELETE CASCADE,
                FOREIGN KEY (deviceUUID) REFERENCES devices(deviceUUID) ON DELETE CASCADE
            );
            """
            cursor.execute(create_pools_devices_table)


            # Create status type lookup table
            create_status_type_lookup_table = """
                            CREATE TABLE status_type_lookup (
                            status_type_id INT NOT NULL,
                            status_type VARCHAR(255),
                            PRIMARY KEY (status_type_id)
                        );
                        """
            cursor.execute(create_status_type_lookup_table)

            # Create Status table
            create_status_table = """
                            CREATE TABLE status_lookup (
                            status_code INT NOT NULL,
                            status_message VARCHAR(255),
                            status_type_id INT NOT NULL,
                            PRIMARY KEY (status_code),
                            FOREIGN KEY (status_type_id) REFERENCES status_type_lookup(status_type_id) ON DELETE CASCADE,
                            INDEX (status_type_id)
                        );
                        """
            cursor.execute(create_status_table)



            cursor.execute("SHOW TABLES;")
            result = cursor.fetchall()
            print(result)

            # Insert into Permissions_Lookup table
            insert_permissions = """
            INSERT INTO permissions_lookup (permissionID, role)
                VALUES 
                    (1, 'ow'),
                    (2, 'admin'),
                    (3, 'user');
            """
            cursor.execute(insert_permissions)

            insert_invite_types = """
            INSERT INTO invite_lookup (inviteID, type)
            VALUES 
            (1, 'single use'),
            (2, 'expires'),
            (3, 'hub');
            """
            cursor.execute(insert_invite_types)

            insert_device_types = """
                INSERT INTO device_lookup (device_type_ID, type)
                VALUES 
                    (1, 'HUB'),
                    (2, 'DIMMABLE_LIGHT_ENDPOINT'),
                    (3, 'ENCODER_CONTROLLER'),
                    (4, 'PIR_CONTROLLER');
            """
            cursor.execute(insert_device_types)

            # Insert into Permissions_Lookup table
            insert_operations = """
                INSERT INTO audit_operation_lookup (operationID, operation_name)
                    VALUES 
                        (1, 'UPDATE'),
                        (2, 'DELETE'),
                        (3, 'INSERT');
                """
            cursor.execute(insert_operations)

            # Insert into warning_lookup table
            insert_status = """
                                   INSERT INTO status_type_lookup (status_type_id, status_type )
                                       VALUES 
                                           (1, 'OK'),
                                           (2, 'WARNING'),
                                           (3, 'ERROR');
                                   """
            cursor.execute(insert_status)

            # Insert into status_lookup table
            insert_status_lookup = f"""
                                   INSERT INTO status_lookup (status_code,status_message, status_type_id )
                                       VALUES ( %s, %s, %s)      
                               """

            values = [(entry["status_code"], entry["status_message"], entry["status_type_id"])
                      for entry in status_lookup_data]

            cursor.executemany(insert_status_lookup, values)

            # Commit the changes and close the connection
            conn.commit()

    except Exception as e:
        print(e)
        cursor.close()
        conn.close()
        raise ValueError(f"Error inserting into DB {e}")
    finally:
        cursor.close()
        conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Tables Created Successfully!')
    }
