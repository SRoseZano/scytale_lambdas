import boto3
import json
import logging
import traceback
from botocore.exceptions import ClientError
import zanolambdashelper

cognito_client = zanolambdashelper.helpers.create_client("cognito-idp")

zanolambdashelper.helpers.set_logging('INFO')


def lambda_handler(event, context):
    try:
        auth_token = event['params']['header']['Authorization'] #id token

        body_json = event['body-json']

        # Extract relevant attributes
        access_token = body_json.get('access_token')

        if not access_token:
            raise Exception("No access token provided")

        response = cognito_client.get_user(
            AccessToken=access_token
        )

        mfa_methods = response.get("UserMFASettingList", [])

        software_token_enabled = "SOFTWARE_TOKEN_MFA" in mfa_methods

    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        traceback.print_exc()
        status_value = 500
        body_value = 'Unable to leave organisation'
        if len(e.args) >= 2 and isinstance(e.args[0], int):
            status_value = e.args[0]
            if status_value == 422:
                body_value = e.args[1]
        error_response = {
            'statusCode': status_value,
            'body': body_value,
        }
        return error_response

    return {
        "statusCode": 200,
        "body": json.dumps({
            "software_token_mfa_enabled": software_token_enabled
        })
    }



