import json
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    cognito_client = boto3.client('cognito-idp')
    user_pool_id = 'eu-west-2_TUhwdis6d'
    try:

        # Process the payload variables
        email = event.get("email", "")

        try:
            # Step 1: Find user by email
            response = cognito_client.list_users(
                UserPoolId=user_pool_id,
                Filter=f'email = "{email}"'
            )

            users = response.get('Users', [])
            if not users:
                raise Exception(400, f"No user found with email: {email}")

            username = users[0]['Username']

            # Step 2: Delete user by username
            cognito_client.admin_delete_user(
                UserPoolId=user_pool_id,
                Username=username
            )


        except Exception as e:
            raise Exception(400, e)

    except ClientError as e:
        error_message = f"Error deleting user from cognito pool: {e.response['Error']['Message']}"
        return {
            'statusCode': 500,
            'body': error_message
        }

    return {
        'statusCode': 200,
        'body': 'Account Deleted Successfully'
    }


