import io
import boto3
from datetime import datetime
from PyPDF2 import PdfReader, PdfWriter
from PyPDF2.generic import NameObject, NumberObject
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, HRFlowable, Image
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
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

# ----------------------------
# CONFIG
# ----------------------------
RESOURCES_BUCKET = "scytale-emergency-test-pdf-resources-423623864387-eu-west-2-an"  # template + images
OUTPUT_BUCKET = "scytale-prod-emergency-test-reports-423623864387-eu-west-2-an"  # final PDF
PDF_TEMPLATE_KEY = "EmergencyBatteryPDFTemplate.pdf"
emergency_device_type_id = 5
mailing_list = ['stuart.rose@zanocontrols.co.uk']
sender_email = 'noreply@zanocontrols.co.uk'

s3 = boto3.client("s3")

image_cache = {}


lower_threshold = 3600 * 3
upper_threshold = 3600 * 4

from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

def send_pdf_via_ses(sender_email, recipient_email, pdf_buffer, file_name, subject="Your PDF Report"):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject

    # Empty body
    msg.attach(MIMEText('', 'plain'))

    # Attach PDF
    pdf_part = MIMEApplication(pdf_buffer.getvalue(), _subtype='pdf')
    pdf_part.add_header('Content-Disposition', 'attachment', filename=file_name)
    msg.attach(pdf_part)

    response = boto3.client('ses', region_name='eu-west-2').send_raw_email(
        Source=sender_email,
        Destinations=[recipient_email],
        RawMessage={'Data': msg.as_string()}
    )
    return response



def get_org_name(cursor, org_uuid):
    logging.info("Getting organisations name...")

    sql = f"""
        SELECT 
            organisation_name
        FROM {database_dict['schema']}.{database_dict['organisations_table']}
        WHERE organisationUUID = %s
    """

    cursor.execute(sql, (org_uuid,))
    name = cursor.fetchone()

    if not name:
        raise Exception("Organisation doesnt exist")

    return name


def get_org_functional_test_results(cursor, org_uuid):
    logging.info("Getting organisations monthly emergency device results...")

    sql = f"""
        SELECT 
            deviceUUID,
            result,
            result_timestamp
        FROM {database_dict['schema']}.{database_dict['emergency_functional_test_result_table']}
        WHERE organisationUUID = %s
          AND result_timestamp >= DATE_FORMAT(CURDATE(), '%Y-01-01');
    """

    cursor.execute(sql, (org_uuid,))
    results = cursor.fetchall()

    mapped_results = {}

    if results:

        for row in results:
            device_uuid, result_value, result_time = row

            # Initialise dicts if needed
            if device_uuid not in mapped_results:
                mapped_results[device_uuid] = {}
            if 1 not in mapped_results[device_uuid]:
                mapped_results[device_uuid][1] = {}

            # Map timestamp to result
            mapped_results[device_uuid][1][int(result_time.timestamp())] = result_value

    return mapped_results


def get_org_discharge_test_results(cursor, org_uuid):
    logging.info("Getting organisations yearly emergency device results...")

    sql = f"""
        SELECT 
            deviceUUID,
            discharge_time AS result,
            result_timestamp
        FROM {database_dict['schema']}.{database_dict['emergency_discharge_test_result_table']}
        WHERE organisationUUID = %s
          AND result_timestamp >= DATE_FORMAT(CURDATE(), '%Y-01-01');
    """

    cursor.execute(sql, (org_uuid,))
    results = cursor.fetchall()

    mapped_results = {}

    if results:
        for row in results:
            device_uuid, result_value, result_time = row

            # Initialise dicts if needed
            if device_uuid not in mapped_results:
                mapped_results[device_uuid] = {}
            if 2 not in mapped_results[device_uuid]:
                mapped_results[device_uuid][2] = {}

            # Map timestamp to result
            mapped_results[device_uuid][2][int(result_time.timestamp())] = result_value

    return mapped_results

def merge_device_data(devices, functional_results, discharge_results):

    logging.info("Merging all datasets...")

    merged = []

    for device in devices:
        device_uuid = device["device_uuid"]

        # Base structure
        device_entry = {
            "device_long_address": device["long_address"],
            "device_name": device["device_name"],
            "group_name": device["device_group"],
            "1": {},
            "2": {}
        }

        # Functional results
        if device_uuid in functional_results and 1 in functional_results[device_uuid]:
            for ts, value in functional_results[device_uuid][1].items():
                date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                device_entry["1"][date_str] = value

        # Discharge results
        if device_uuid in discharge_results and 2 in discharge_results[device_uuid]:
            for ts, value in discharge_results[device_uuid][2].items():
                date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                device_entry["2"][date_str] = value

        merged.append(device_entry)

    return merged


def get_org_devices(cursor, org_uuid, device_type_id):
    import logging

    logging.info("Getting organisation's emergency devices...")

    sql = """
        WITH RECURSIVE pool_hierarchy AS (
            -- Start with the pools each device is directly linked to
            SELECT
                d.deviceUUID,
                d.device_name,
                d.long_address,
                p.poolUUID,
                p.pool_name,
                p.parentUUID,
                1 AS depth
            FROM devices d
            JOIN pools_devices pd ON d.deviceUUID = pd.deviceUUID
            JOIN pools p ON pd.poolUUID = p.poolUUID
            WHERE d.organisationUUID = %s
              AND d.device_type_id = %s

            UNION ALL

            -- Traverse up the hierarchy
            SELECT
                ph.deviceUUID,
                ph.device_name,
                ph.long_address,
                parent.poolUUID,
                parent.pool_name,
                parent.parentUUID,
                ph.depth + 1
            FROM pool_hierarchy ph
            JOIN pools parent ON ph.parentUUID = parent.poolUUID
        )
        -- Pick only the "lowest-level" pool per device
        SELECT ph.deviceUUID, ph.long_address, ph.device_name, ph.pool_name
        FROM pool_hierarchy ph
        JOIN (
            SELECT deviceUUID, MAX(depth) AS max_depth
            FROM pool_hierarchy
            GROUP BY deviceUUID
        ) deepest 
        ON ph.deviceUUID = deepest.deviceUUID AND ph.depth = deepest.max_depth
        ORDER BY ph.device_name;
    """

    # Execute with both org_uuid and device_type_id
    cursor.execute(sql, (org_uuid, device_type_id))
    results = cursor.fetchall()

    # Map results into a list of dictionaries
    mapped_results = []
    for row in results:
        device_uuid, long_address, device_name, device_group = row
        mapped_results.append({
            "device_uuid": device_uuid,
            "long_address": long_address,
            "device_name": device_name,
            "device_group": device_group
        })

    return mapped_results



def fetch_s3_file(bucket, key):

    """Download file from S3 into memory"""
    logging.info("Loading pdf resources ...")

    obj = s3.get_object(Bucket=bucket, Key=key)

    logging.info("Done ...")
    return io.BytesIO(obj["Body"].read())




def s3_image_to_rl_image(image_key, width=12, height=12):
    """Fetch image from S3 with caching"""
    if image_key not in image_cache:
        image_cache[image_key] = fetch_s3_file(RESOURCES_BUCKET, image_key)

    # IMPORTANT: create a fresh BytesIO each time (ReportLab consumes streams)
    return Image(io.BytesIO(image_cache[image_key].getvalue()), width=width, height=height)



def calculate_health_percentage(seconds):
    if seconds <= lower_threshold:
        return 0
    elif seconds >= upper_threshold:
        return 100
    else:
        return round((seconds - lower_threshold) / (upper_threshold - lower_threshold) * 100)


def format_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b")
    except:
        return date_str


def build_horizontal_wrapped_tables(test_data, is_functional=True, page_width=A4[0]):
    logging.info("Adding wrapped mini tables...")

    sorted_items = sorted(test_data.items())
    current_row = []
    rows_tables = []

    LEFT_MARGIN = 20
    SPACER_WIDTH = 20
    current_width = LEFT_MARGIN
    max_width = page_width - 40

    for timestamp, value in sorted_items:
        if is_functional:
            icon_key = "BatteryReport-114.png" if value else "BatteryReport-116.png"
            mini_table_data = [[format_date(timestamp), s3_image_to_rl_image(icon_key)]]
            mini_table_width = 60
        else:
            if value <= lower_threshold:
                icon_key = "BatteryReport-116.png"
            elif value >= upper_threshold:
                icon_key = "BatteryReport-114.png"
            else:
                icon_key = "BatteryReport-115.png"

            hours, mins = value // 3600, (value % 3600) // 60
            time_str = f"{hours} Hours {mins} Mins"
            health_percent = max(0, min(100, int((value - 10800) / 3600 * 100)))

            mini_table_data = [
                ["Test Date", "Battery Health", "Discharge Time", "Result"],
                [format_date(timestamp), f"{health_percent}%", time_str,
                 s3_image_to_rl_image(icon_key)]
            ]
            mini_table_width = 100

        mini_table = Table(mini_table_data)
        style_list = [
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.black),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ]
        if not is_functional:
            style_list.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#CBEDF4")))
        mini_table.setStyle(style_list)

        if current_width + mini_table_width > max_width:
            if current_row:
                row_table = Table([current_row], hAlign="LEFT")
                row_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
                rows_tables.append(row_table)
            current_row = []
            current_width = LEFT_MARGIN

        current_row.append(mini_table)
        current_width += mini_table_width + SPACER_WIDTH

    if current_row:
        row_table = Table([current_row], hAlign="LEFT")
        row_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        rows_tables.append(row_table)

    return rows_tables

def generate_final_pdf_buffer(example_data, org_name, template_pdf_buffer):

    logging.info("Generating pdf to buffer...")

    reader = PdfReader(template_pdf_buffer)
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

    # Fill form fields
    field_values = {
        "org_name": org_name,
        "date": f"01/01/{datetime.today().year} - {datetime.today().strftime('%d/%m/%Y')}",
    }
    writer.update_page_form_field_values(writer.pages[0], field_values)

    # Flatten form fields
    for page in writer.pages:
        if "/Annots" in page:
            for annot in page["/Annots"]:
                field = annot.get_object()
                if "/T" in field:
                    current_flags = field.get("/Ff", 0)
                    field.update({NameObject("/Ff"): NumberObject(current_flags | 1)})

    filled_pdf_buffer = io.BytesIO()
    writer.write(filled_pdf_buffer)
    filled_pdf_buffer.seek(0)

    tables_buffer = io.BytesIO()
    doc = SimpleDocTemplate(tables_buffer, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()

    elements.append(Spacer(1, 175))

    for device in example_data:
        # Device Table
        device_table_data = [
            ["Device ID", "Device Name", "Group Name"],
            [
                device["device_long_address"],
                device["device_name"],
                device["group_name"]
            ]
        ]
        device_table = Table(device_table_data, colWidths=[150, 150, 150], hAlign='LEFT')
        device_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#CBEDF4")),
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]))
        elements.append(device_table)
        elements.append(Spacer(1, 8))

        # Functional Tests
        elements.append(Paragraph("Functional Tests", styles["Heading4"]))
        if device.get("1"):
            func_rows = build_horizontal_wrapped_tables(device["1"], True)
            for row in func_rows:
                elements.append(row)
                elements.append(Spacer(1, 2))
        else:
            elements.append(Paragraph("No data", styles["Normal"]))
        elements.append(Spacer(1, 10))

        # Discharge Tests
        elements.append(Paragraph("Discharge Tests", styles["Heading4"]))
        if device.get("2"):
            discharge_rows = build_horizontal_wrapped_tables(device["2"], False)
            for row in discharge_rows:
                elements.append(row)
                elements.append(Spacer(1, 2))
        else:
            elements.append(Paragraph("No data", styles["Normal"]))

        elements.append(Spacer(1, 20))
        elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
        elements.append(Spacer(1, 20))

    doc.build(elements)
    tables_buffer.seek(0)

    # 3️⃣ Merge PDFs
    form_pdf = PdfReader(filled_pdf_buffer)
    tables_pdf = PdfReader(tables_buffer)
    final_writer = PdfWriter()

    first_page_form = form_pdf.pages[0]
    first_page_table = tables_pdf.pages[0]
    first_page_form.merge_page(first_page_table)
    final_writer.add_page(first_page_form)

    for page in tables_pdf.pages[1:]:
        final_writer.add_page(page)
    for page in form_pdf.pages[1:]:
        final_writer.add_page(page)

    final_buffer = io.BytesIO()
    final_writer.write(final_buffer)
    final_buffer.seek(0)

    return final_buffer

def lambda_handler(event, context):
    try:
        database_token = zanolambdashelper.helpers.generate_database_token(rds_client, rds_user, rds_host, rds_port,
                                                                           rds_region)

        conn = zanolambdashelper.helpers.initialise_connection(rds_user, database_token, rds_db, rds_host, rds_port)
        conn.autocommit = False

        auth_token = event['params']['header']['Authorization']
        body_json = event['body-json']
        user_email = zanolambdashelper.helpers.decode_cognito_id_token(auth_token)

        with conn.cursor() as cursor:

            user_uuid = zanolambdashelper.helpers.get_user_details_by_email(cursor,
                                                                            database_dict['schema'],
                                                                            database_dict['users_table'],
                                                                            user_email)
            org_uuid = zanolambdashelper.helpers.get_user_organisation_details(cursor,
                                                                               database_dict['schema'],
                                                                               database_dict[
                                                                                   'users_organisations_table'],
                                                                               user_uuid)

            # validate precursors to running this command
            zanolambdashelper.helpers.is_user_org_owner(cursor, database_dict['schema'],
                                                        database_dict['users_organisations_table'], user_uuid,
                                                        org_uuid)

            org_name, = get_org_name(cursor, org_uuid)

            org_emergency_devices = get_org_devices(cursor,org_uuid,emergency_device_type_id)
            functional_results = get_org_functional_test_results(cursor, org_uuid)
            discharge_results = get_org_discharge_test_results(cursor, org_uuid)
            device_data_merged = merge_device_data(org_emergency_devices,functional_results,discharge_results)

            template_pdf_buffer = fetch_s3_file(RESOURCES_BUCKET, PDF_TEMPLATE_KEY)

            final_buffer = generate_final_pdf_buffer(
                device_data_merged,
                org_name,
                template_pdf_buffer
            )

            # Timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            current_year = datetime.now().year

            output_key = f"{org_uuid}/{current_year}/emergency_lighting_report_{timestamp}.pdf"

            logging.info("Upload to s3...")
            # Upload to S3
            s3.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=output_key,
                Body=final_buffer.getvalue(),
                ContentType="application/pdf"
            )
            logging.info("Send to user email...")
            send_pdf_via_ses(sender_email, user_email, final_buffer, f"emergency_lighting_report_{timestamp}.pdf")

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to generate pdf'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422 or status_value == 403:  # if 422 then validation error
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
        'body': 'Test result pdf generated successfully',
    }

