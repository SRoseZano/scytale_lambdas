import math

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
test_type_id = 2

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
    base = datetime.combine(date, datetime.min.time())
    test_time = base + preferred_time
    return test_time



def get_emergency_devices(cursor):
    logging.info("Fetching emergency devices and their most recent test result...")

    sql = f"""

            with ranked_results as (
                SELECT 
                    organisationUUID,
                    deviceUUID,
                    discharge_time,
                    result_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY organisationUUID, deviceUUID
                        ORDER BY result_timestamp DESC
                    ) AS rn
                FROM {database_dict['schema']}.{database_dict['emergency_discharge_test_result_table']}
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

            # Compute 1 year after last result
            one_year_after_result = result + relativedelta(years=1)

            if result < now - relativedelta(years=1):
                #  Result older than 1 year
                new_test_time = tonight_at(preferred_time)

            elif test_time is None or test_time < result:
                # Test time is missing or older than last result
                new_test_time = one_year_after_result

            elif test_time < now:
                # Scheduled test already passed, move forward 1 year from last result
                new_test_time = one_year_after_result

            elif test_time > one_year_after_result:
                # Test scheduled too far ahead
                new_test_time = one_year_after_result

            else:
                new_test_time = test_time

        rows.append({
            "deviceUUID": deviceUUID,
            "organisationUUID": orgUUID,
            "test_type_id": 2,
            "test_time": at_preferred_time(new_test_time, preferred_time),
            "pref_test_time": preferred_time,
            "result_timestamp": result
        })

    return rows


def balance_schedule(rows):

    org_groups = {}

    for row in rows:  #get all of the orgs in the database and add the rows applicable to that org for balancing
        org = row["organisationUUID"]
        org_groups.setdefault(org, []).append(row)

    for org, devices in org_groups.items(): #itterate over each org and the devices in it

        total_devices = len(devices) #get total amount of devices
        max_per_day = math.ceil(total_devices / 365) #get the maximum amount of devices to be allowed per day


        day_map = {}

        for d in devices: #for every device in org
            day = d["test_time"].date() #get the date of the test and append the device to said date
            day_map.setdefault(day, []).append(d)

        # ensure all days exist by looping through and setting empty string as result if not already in day_map
        for i in range(365):
            day = now.date() + timedelta(days=i)
            day_map.setdefault(day, [])

        # loop through days and find ones with associated devices over capacity
        for day, day_devices in day_map.items():

            if len(day_devices) <= max_per_day: #if less or same skip
                continue

            overflow = len(day_devices) - max_per_day #get the amount of device tests over the calculated max

            #only select devices that have a historic to be moved (otherwise it has to be tested that day)
            movable = [
                d for d in day_devices
                if d["result_timestamp"] is not None
            ]

            # prioritise oldest last test by bringing device with oldest last test result to front (ensuring its moved first)
            movable.sort(key=lambda x: x["result_timestamp"])

            for device in movable[:overflow]: #for each device in the list of movable devices up to the overflow limit

                result = device["result_timestamp"] #get devices last test and calc the last date the test could possibly be run and still be compliant
                max_date = result + timedelta(days=364) #(364 days because worried day balancer logic further below might move device test a few hours over the year limit )

                moved = False

                # loop through all potential days and device test on said day
                for candidate_day, candidate_devices in [(day, devices) for day, devices in sorted(day_map.items()) if len(devices) < max_per_day]:

                    if candidate_day > max_date.date(): #if adding the overflowed device to candidate day would put it over the 364 day limit then skip
                        continue

                    device["test_time"] = datetime.combine( #else set test time to the new date at the original hour
                        candidate_day,
                        device["test_time"].time()
                    )

                    candidate_devices.append(device) #add device to the day list (updates count for next itteration)
                    day_devices.remove(device) #remove the device from origional day list it was associated with

                    moved = True
                    break

                if not moved: #if no test has been moved
                    continue

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
            spreaded_tests = balance_schedule(devices_test_time)
            set_new_schedule(cursor, spreaded_tests)
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
