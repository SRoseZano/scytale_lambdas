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
    "status_code": 2000,
    "status_message": "OK",
    "status_type_id": 1
},
    {
        "status_code": 3000,
        "status_message": "WARNING",
        "status_type_id": 2
    },
    {
        "status_code": 4000,
        "status_message": "ERROR",
        "status_type_id": 3
    },
    {
        "status_code": 3005,
        "status_message": "Initialising",
        "status_type_id": 2
    },
    {
        "status_code": 3010,
        "status_message": "Failed to communicate with device",
        "status_type_id": 2
    },

    {
        "status_code": 4010,
        "status_message": "Failed to communicate with device",
        "status_type_id": 3
    },
    {
        "status_code": 4100,
        "status_message": "Driver output short circuit",
        "status_type_id": 3
    },
    {
        "status_code": 4101,
        "status_message": "Driver output open circuit",
        "status_type_id": 3
    },
]

# Device type events and actions data
device_type_data = {
    3: {  # Device type
        "events": [
            {"event_number": 1, "event_name": "Press"},
            {"event_number": 2, "event_name": "Turn Up"},
            {"event_number": 3, "event_name": "Turn Down"},
            {"event_number": 4, "event_name": "Hold"},
            {"event_number": 5, "event_name": "Hold Turn Up"},
            {"event_number": 6, "event_name": "Hold Turn Down"}
        ],
        "actions": []
    },
    4: {  # Device type
        "events": [
            {"event_number": 1, "event_name": "Presence Detected"},
            {"event_number": 2, "event_name": "Presence Gone"}
        ],
        "actions": []
    },
    2: {  # Device type
        "events": [],
        "actions": [
            {"action_number": 100, "action_name": "Light On"},
            {"action_number": 101, "action_name": "Light On For"},
            {"action_number": 102, "action_name": "Light Off"},
            {"action_number": 103, "action_name": "Light Off For"},
            {"action_number": 104, "action_name": "Light Set"},
            {"action_number": 105, "action_name": "Light Toggle"},
            {"action_number": 106, "action_name": "Brightness Set"}
        ]
    },
    5: {  # Device type
        "events": [],
        "actions": [
            {"action_number": 100, "action_name": "Light On"},
            {"action_number": 101, "action_name": "Light On For"},
            {"action_number": 102, "action_name": "Light Off"},
            {"action_number": 103, "action_name": "Light Off For"},
            {"action_number": 104, "action_name": "Light Set"},
            {"action_number": 105, "action_name": "Light Toggle"},
            {"action_number": 106, "action_name": "Brightness Set"},
            {"action_number": 107, "action_name": "Brightness Up"},
            {"action_number": 108, "action_name": "Brightness Down"},
            {"action_number": 109, "action_name": "Battery Charging On"},
            {"action_number": 110, "action_name": "Battery Charging Off"},
            {"action_number": 111, "action_name": "Battery Charging Set"},
            {"action_number": 112, "action_name": "Battery Brightness Set"}
        ]
    }
}



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
                status_lookup,
                hub_radios,
                device_type_default_mappings,
                device_type_actions,
                device_type_events,
                event_mapping_controls_lookup,
                device_status_log
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
                    record_id VARCHAR(36),               
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
                stripe_sub_id VARCHAR(50),
                PRIMARY KEY (organisationUUID)
            );
            """
            cursor.execute(create_organisation_table)

            # Create Pools table
            create_pools_table = """
            CREATE TABLE pools (
                poolUUID VARCHAR(36) NOT NULL,
                organisationUUID VARCHAR(36) NOT NULL,
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
                organisationUUID VARCHAR(36) NOT NULL,
                target_email VARCHAR(255),
                inviteID INT NOT NULL,
                valid_until TIMESTAMP,
                FOREIGN KEY (organisationUUID) REFERENCES organisations(organisationUUID) ON DELETE CASCADE,
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
                current_firmware VARCHAR(36) DEFAULT '0.0.0',
                target_firmware VARCHAR(36) DEFAULT '1.0.0',
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
                associated_hub VARCHAR(36) NOT NULL,
                registrant VARCHAR(255) NOT NULL,
                device_name VARCHAR(255) NOT NULL,
                organisationUUID VARCHAR(36) NOT NULL,
                device_type_ID INT NOT NULL,
                PRIMARY KEY (deviceUUID),
                UNIQUE (long_address),
                UNIQUE (short_address),
                FOREIGN KEY (organisationUUID) REFERENCES organisations(organisationUUID) ON DELETE CASCADE,
                FOREIGN KEY (device_type_ID) REFERENCES device_lookup(device_type_ID) ON DELETE CASCADE,
                FOREIGN KEY (associated_hub) REFERENCES hubs(hubUUID) ON DELETE CASCADE
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

            # Create hub radio table
            create_hub_radios_table = """
                                        CREATE TABLE hub_radios (
                                        hubUUID VARCHAR(36) NOT NULL,
                                        long_address VARCHAR(16) NOT NULL,
                                        short_address VARCHAR(4) NOT NULL,
                                        PRIMARY KEY (hubUUID, long_address),
                                        FOREIGN KEY (hubUUID) REFERENCES hubs(hubUUID) ON DELETE CASCADE,
                                        INDEX (hubUUID)
                                    );
                                    """

            cursor.execute(create_hub_radios_table)

            # Create device type events table
            create_device_type_events_table = """
                                       CREATE TABLE device_type_events (
                                          event_ID INT AUTO_INCREMENT PRIMARY KEY,
                                          device_type_ID INT NOT NULL,
                                          event_number INT NOT NULL,
                                          event_name VARCHAR(255) NOT NULL,
                                          UNIQUE (device_type_ID, event_number),
                                          FOREIGN KEY (device_type_ID)
                                            REFERENCES device_lookup(device_type_ID)
                                            ON DELETE CASCADE
                                        );

                                   """

            cursor.execute(create_device_type_events_table)

            # Create device type actions table
            create_device_type_actions_table = """
                                       CREATE TABLE device_type_actions (
                                          action_ID INT AUTO_INCREMENT PRIMARY KEY,
                                          device_type_ID INT NOT NULL,
                                          action_number INT NOT NULL,
                                          action_name VARCHAR(255) NOT NULL,
                                          UNIQUE (device_type_ID, action_number),
                                          FOREIGN KEY (device_type_ID)
                                            REFERENCES device_lookup(device_type_ID)
                                            ON DELETE CASCADE
                                        );

                                   """

            cursor.execute(create_device_type_actions_table)

            # Create device type default mappings table
            create_device_type_default_mappings_table = """
                                       CREATE TABLE device_type_default_mappings (
                                          mapping_ID INT AUTO_INCREMENT PRIMARY KEY,
                                          event_ID INT NOT NULL,
                                          action_ID INT NOT NULL,
                                          action_data INT,
                                          priority INT NOT NULL,
                                          sequence INT NOT NULL,
                                          time_days TINYINT UNSIGNED NOT NULL,
                                          time_start SMALLINT UNSIGNED NOT NULL,
                                          time_stop SMALLINT UNSIGNED NOT NULL,
                                          control_type_id INT,
                                          FOREIGN KEY (event_ID)
                                            REFERENCES device_type_events(event_ID)
                                            ON DELETE CASCADE,
                                          FOREIGN KEY (action_ID)
                                            REFERENCES device_type_actions(action_ID)
                                            ON DELETE CASCADE,
                                          FOREIGN KEY (control_type_id)
                                            REFERENCES device_type_actions(control_ID)
                                            ON DELETE CASCADE
                                        );
                                   """

            cursor.execute(create_device_type_default_mappings_table)

            # Create event mapping control type lookup table
            create_event_mapping_controllers_table = """
                                       CREATE TABLE event_mapping_controls_lookup (
                                          control_ID INT PRIMARY KEY,
                                          control_type VARCHAR(255) NOT NULL
                                        );
                                   """

            cursor.execute(create_event_mapping_controllers_table)

            #Create device staus log table
            create_device_status_log_table = """
            CREATE TABLE device_status_log (
                logUUID SERIAL PRIMARY KEY, 
                organisationUUID VARCHAR(36) NOT NULL,
                organisation_name VARCHAR(255) NOT NULL,
                deviceUUID VARCHAR(36) NOT NULL,
                device_type_ID INT NOT NULL,
                device_long_address VARCHAR(16),
                associated_hubUUID VARCHAR(36),
                associated_hub_serial VARCHAR(64),
                status_code INT NOT NULL,
                status_message VARCHAR(255),
                status_type VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX (organisationUUID),
                INDEX (status_code),
                INDEX (associated_hubUUID),
                INDEX (associated_hub_serial),
                INDEX (deviceUUID),
                INDEX (device_long_address)
            );
            """

            cursor.execute(create_device_status_log_table)

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
                    (4, 'PIR_CONTROLLER'),
                    (5, 'EMERGENCY_LIGHT_ENDPOINT');
            """
            cursor.execute(insert_device_types)

            insert_event_mapping_controls = """
                   INSERT INTO event_mapping_controls_lookup (control_ID, control_type)
                       VALUES 
                           (1, 'Toggle'),
                           (2, 'Slider'),
                           (3, 'Time Picker');
                   """
            cursor.execute(insert_event_mapping_controls)

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

            # insert device type related actions and events ENTER DEFAULT MAPPINGS MANUALLY
            for device_type_id, data in device_type_data.items():
                # Insert events
                for event in data["events"]:
                    cursor.execute(
                        "INSERT INTO device_type_events (device_type_ID, event_number, event_name) VALUES (%s, %s, %s)",
                        (device_type_id, event["event_number"], event["event_name"])
                    )
                # Insert actions
                for action in data["actions"]:
                    cursor.execute(
                        "INSERT INTO device_type_actions (device_type_ID, action_number, action_name) VALUES (%s, %s, %s)",
                        (device_type_id, action["action_number"], action["action_name"])
                    )

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
