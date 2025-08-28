import json
import boto3
from botocore.exceptions import ClientError
import firebase_admin
import zanolambdashelper
from firebase_admin import credentials, messaging
import os

firebase_credentials = zanolambdashelper.helpers.get_firebase_creds()

if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_credentials)  # Update the path
    firebase_admin.initialize_app(cred)


def send_message_to_topic(msg_topic, status_code_type_id, device_name, device_type_ID, device_uuid):
    # Determine the message body based on the status code
    if status_code_type_id == 2:
        msg_body = f"{'Hub' if device_type_ID == 1 else 'Device'}: {device_name}\n\nHas encountered a warning."
    elif status_code_type_id == 3:
        msg_body = f"{'Hub' if device_type_ID == 1 else 'Device'}: {device_name}\n\nHas encountered an error."
    else:
        msg_body = f"{'Hub' if device_type_ID == 1 else 'Device'}: {device_name}\n\nHas encountered an unknown status."

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

    # Send the message
    try:
        response = messaging.send(message)
        print('Successfully sent message:', response)
    except Exception as e:
        print(f"Error sending message: {e}")


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
