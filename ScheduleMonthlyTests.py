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
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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

emergency_light_device_id = 5
test_type_id = 1

now = datetime.now()

def set_new_schedule(cursor, device_schedules):
    logging.info("Setting new emergency device test schedules...")

    if not device_schedules:
        logging.info("No schedules to update.")
        return

    #if schedule doesnt exist then insert else update the test_time value
    sql = f"""
        INSERT INTO {database_dict['schema']}.{database_dict['emergency_test_schedule_table']}
        (organisationUUID, deviceUUID, test_type_id, test_time)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            test_time = VALUES(test_time)
    """

    # Prepare the values as a list of tuples matching the %s order in SQL
    values = [
        (
            row["organisationUUID"],
            row["deviceUUID"],
            row["test_type_id"],
            row["test_time"]
        )
        for row in device_schedules
    ]


    cursor.executemany(sql, values)
    logging.info(f"Updated {len(values)} emergency device test schedules.")

def tonight_at(preferred_time):
    today = now.date()
    base = datetime.combine(today, datetime.min.time())
    test_time = base + preferred_time

    if test_time < now:
        test_time += timedelta(days=1)

    return test_time

def at_preferred_time(date, preferred_time):
    return date.replace(
        hour=preferred_time.hour,
        minute=preferred_time.minute,
        second=0,
        microsecond=0
    )

def get_emergency_devices(cursor):
    logging.info("Fetching emergency devices and their most recent test result...")

    sql = f"""

            with ranked_results as (
                SELECT 
                    organisationUUID,
                    deviceUUID,
                    result,
                    result_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY organisationUUID, deviceUUID
                        ORDER BY result_timestamp DESC
                    ) AS rn
                FROM {database_dict['schema']}.{database_dict['emergency_functional_test_result_table']}
            )
            SELECT DISTINCT a.deviceUUID, a.organisationUUID, b.preferred_test_time, c.test_time, d.result_timestamp
            FROM {database_dict['schema']}.{database_dict['devices_table']} a
            INNER JOIN {database_dict['schema']}.{database_dict['organisations_table']} b on a.organisationUUID = b.organisationUUID
            LEFT JOIN {database_dict['schema']}.{database_dict['emergency_test_schedule_table']} c  ON a.deviceUUID = c.deviceUUID AND a.organisationUUID = c.organisationUUID AND c.test_type_id = %s
            LEFT JOIN ranked_results d ON a.organisationUUID = d.organisationUUID AND a.deviceUUID = d.deviceUUID AND d.rn = 1
            WHERE a.device_type_ID = %s 
    """
    cursor.execute(sql,(test_type_id, emergency_light_device_id))
    result = cursor.fetchall()
    return result

def calculate_test_times(test_data):
    rows = []

    for row in test_data:

        deviceUUID = row[0]
        orgUUID = row[1]
        preferred_time = row[2]
        test_time = row[3]
        result = row[4]

        new_test_time = None

        # Rule 1: No result
        if result is None:
            new_test_time = tonight_at(preferred_time)

        else:

            # Compute 1 month after last result
            one_month_after_result = result + relativedelta(months=1)

            if result < now - relativedelta(months=1):
                #  Result older than 1 month
                new_test_time = tonight_at(preferred_time)

            elif test_time is None or test_time < result:
                # Test time is missing or older than last result
                new_test_time = one_month_after_result

            elif test_time < now:
                # Scheduled test already passed, move forward 1 month from last result
                new_test_time = one_month_after_result

            elif test_time > one_month_after_result:
                # Test scheduled too far ahead
                new_test_time = one_month_after_result

            else:
                continue

        rows.append({
            "deviceUUID": deviceUUID,
            "organisationUUID": orgUUID,
            "test_type_id": 1,
            "test_time": at_preferred_time(new_test_time, preferred_time),
        })

    return rows



def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        with conn.cursor() as cursor:

            emergency_devices = get_emergency_devices(cursor)
            devices_test_time = calculate_test_times(emergency_devices)
            set_new_schedule(cursor,devices_test_time)
            conn.commit()

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to update schedule'
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
        'body': 'Schedules Updated Successfully'
    }
