import json
import mysql.connector
import boto3
import os


rds_host ='zano-controls-rds-proxy.proxy-c3yw8wgeiwfk.eu-west-2.rds.amazonaws.com'
rds_port = 3306
rds_db = 'zanocontrols'
rds_user = 'zanoadmin'


rds_client = boto3.client('rds')



def lambda_handler(event, context):
    
    try:
        database_token = rds_client.generate_db_auth_token(
        DBHostname=rds_host,
        Port=rds_port,
        DBUsername=rds_user,
        Region=os.environ['AWS_REGION']
        )
    
        conn = mysql.connector.connect(user=rds_user, password=database_token, host=rds_host, database=rds_db, port=rds_port)
        cursor = conn.cursor()
        
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
             device_lookup
        """
        cursor.execute(drop_tables)

        
        # Create Users table
        create_users_table = """
            CREATE TABLE users (
            userID INT AUTO_INCREMENT PRIMARY KEY,
            userUUID VARCHAR(36) NOT NULL DEFAULT (UUID()),
            email VARCHAR(255) NOT NULL UNIQUE,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            birthdate DATE NOT NULL,
            identity_pool_id VARCHAR(255) DEFAULT NULL,
            zone_info VARCHAR(100) NOT NULL,
            locale VARCHAR(20) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            hub_user BOOL NOT NULL DEFAULT 0,
            UNIQUE (userUUID),
            INDEX (email),
            INDEX (userUUID)
        );
        """
        cursor.execute(create_users_table)
        
        # Create organisation table
        create_organisation_table = """
        CREATE TABLE organisations (
            organisationID INT AUTO_INCREMENT PRIMARY KEY,
            organisationUUID VARCHAR(36) NOT NULL DEFAULT (UUID()),
            organisation_name VARCHAR(255) NOT NULL,
            associated_policy VARCHAR(255) NOT NULL,
            address_line_1 VARCHAR(255) NOT NULL,
            address_line_2 VARCHAR(255),
            city VARCHAR(50) NOT NULL,
            county VARCHAR(50) NOT NULL,
            postcode VARCHAR(20) NOT NULL,
            phone_no VARCHAR(15) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (organisationUUID),
            INDEX (organisationUUID)
        );
        """
        cursor.execute(create_organisation_table)
        
        # Create Pools table
        create_pools_table = """
        CREATE TABLE pools (
            poolID INT AUTO_INCREMENT PRIMARY KEY,
            poolUUID VARCHAR(36) NOT NULL DEFAULT (UUID()),
            organisationID INT NOT NULL,
            pool_name VARCHAR(100) NOT NULL,
            parentID INT,
            UNIQUE (poolUUID),
            INDEX (poolUUID),
            FOREIGN KEY (organisationID) REFERENCES organisations(organisationID) ON DELETE CASCADE
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
        
        #create invite type lookup table
        create_invite_lookup_table = """
        CREATE TABLE invite_lookup (
            inviteID INT PRIMARY KEY,
            type VARCHAR(100) NOT NULL
        );
        """
        cursor.execute(create_invite_lookup_table)
        
        #Create org invite table
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
            userID INT NOT NULL,
            organisationID INT NOT NULL,
            permissionID INT NOT NULL,
            PRIMARY KEY (userID, organisationID),
            FOREIGN KEY (userID) REFERENCES users(userID) ON DELETE CASCADE,
            FOREIGN KEY (organisationID) REFERENCES organisations(organisationID) ON DELETE CASCADE,
            FOREIGN KEY (permissionID) REFERENCES permissions_lookup(permissionID) ON DELETE CASCADE
        );
        """
        cursor.execute(create_users_organisations_table)
        
        # Create Pools_Users table
        create_pools_users_table = """
        CREATE TABLE pools_users (
            poolID INT NOT NULL,
            userID INT NOT NULL,
            PRIMARY KEY (poolID, userID),
            FOREIGN KEY (poolID) REFERENCES pools(poolID) ON DELETE CASCADE,
            FOREIGN KEY (userID) REFERENCES users(userID) ON DELETE CASCADE
        );
        """
        cursor.execute(create_pools_users_table)
        
        # Create Hubs table
        create_hubs_table = """
        CREATE TABLE hubs (
            hubID INT AUTO_INCREMENT PRIMARY KEY,
            hubUUID VARCHAR(36) NOT NULL DEFAULT (UUID()),
            serial VARCHAR(64) NOT NULL,
            registrant VARCHAR(255) NOT NULL,
            hub_name VARCHAR(255) NOT NULL,
            organisationID INT NOT NULL,
            device_type_ID INT NOT NULL,
            current_firmware VARCHAR(36) NOT NULL,
            target_firmware VARCHAR(36),
            UNIQUE (hubUUID),
            UNIQUE (serial),
            INDEX (hubUUID),
            FOREIGN KEY (organisationID) REFERENCES organisations(organisationID) ON DELETE CASCADE,
            FOREIGN KEY (device_type_ID) REFERENCES device_lookup(device_type_ID) ON DELETE CASCADE
        );

        """
        cursor.execute(create_hubs_table)
        
        # Create Devices table
        create_devices_table = """
        CREATE TABLE devices (
            deviceID INT AUTO_INCREMENT PRIMARY KEY,
            deviceUUID VARCHAR(36) NOT NULL DEFAULT (UUID()),
            long_address VARCHAR(16) NOT NULL,
            short_address VARCHAR(4) NOT NULL,
            associated_hub VARCHAR(64) NOT NULL,
            registrant VARCHAR(255) NOT NULL,
            device_name VARCHAR(255) NOT NULL,
            organisationID INT NOT NULL,
            device_type_ID INT NOT NULL,
            UNIQUE (deviceUUID),
            UNIQUE (long_address),
            UNIQUE (short_address),
            INDEX (deviceUUID),
            FOREIGN KEY (organisationID) REFERENCES organisations(organisationID) ON DELETE CASCADE,
            FOREIGN KEY (device_type_ID) REFERENCES device_lookup(device_type_ID) ON DELETE CASCADE,
            FOREIGN KEY (associated_hub) REFERENCES hubs(serial) ON DELETE CASCADE
        );

        """
        cursor.execute(create_devices_table)
        
        # Create Pools_Devices table
        create_pools_devices_table = """
        CREATE TABLE pools_devices (
            poolID INT NOT NULL,
            deviceID INT NOT NULL,
            PRIMARY KEY (poolID, deviceID),
            FOREIGN KEY (poolID) REFERENCES pools(poolID) ON DELETE CASCADE,
            FOREIGN KEY (deviceID) REFERENCES devices(deviceID) ON DELETE CASCADE
        );
        """
        cursor.execute(create_pools_devices_table)
        
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
