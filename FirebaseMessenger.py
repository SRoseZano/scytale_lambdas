import json
import boto3
from botocore.exceptions import ClientError
import firebase_admin
import zanolambdashelper
from firebase_admin import credentials, messaging
import os

firebase_credentials = zanolambdashelper.helpers.get_firebase_creds()

zanolambdashelper.helpers.set_logging('INFO')

if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_credentials)  # Update the path
    firebase_admin.initialize_app(cred)


def send_message_to_topic(msg_topic, status_code_type_id, device_name, device_type_ID, device_uuid):
    # Determine the message body based on the status code type ID using a match statement (Python 3.10+)
    match status_code_type_id:
        case 2:
            status_suffix = "Has encountered a warning."
        case 3:
            status_suffix = "Has encountered an error."
        case _:  # The default case
            status_suffix = "Has encountered an unknown status."

    msg_body = f"{'Hub' if device_type_ID == 1 else 'Device'}: {device_name}\n\n{status_suffix}"

    # Build the message with notification and data (payload)
    message = messaging.Message(
        notification=messaging.Notification(
            title=f"{'Hub' if device_type_ID == 1 else 'Device'} Status Alert",
            body=msg_body,
        ),
        data={  # Add device_type_ID to the payload
            'device_type_id': f"{device_type_ID}",
            'device_uuid': f"{device_uuid}",
            'status_code_type_id': f"{status_code_type_id}"
        },
        topic=msg_topic,
    )

    response = messaging.send(message)
    logging.info(f'Successfully sent message: {response}')


def lambda_handler(event, context):
    try:

        firebase_topic = event.get("topic", "")
        status_code_type_id = event.get("status_code_type_id", "")
        device_name = event.get("device_name", "")
        device_type_ID = event.get("device_type_ID", "")
        device_uuid = event.get("device_uuid", "")

        send_message_to_topic(firebase_topic, status_code_type_id, device_name, device_type_ID, device_uuid)


    except ClientError as e:
        error_message = f"Error sending message: {e}"
        return {
            'statusCode': 500,
            'body': error_message
        }

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Message Sent!')
    }
