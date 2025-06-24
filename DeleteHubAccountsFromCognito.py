import json
import boto3
from botocore.exceptions import ClientError

cognito = boto3.client('cognito-idp')
USER_POOL_ID = 'eu-west-2_TUhwdis6d'


def get_all_cognito_users():
    users = []
    paginator = cognito.get_paginator('list_users')
    for page in paginator.paginate(UserPoolId=USER_POOL_ID):
        users.extend(page['Users'])
    return users


def delete_unused_cognito_users(cognito_users, inactive_emails):
    deleted_emails = []

    print(f"Cognito Users : {cognito_users}")
    print(f"Inactive Hub Users : {inactive_emails}")

    for user in cognito_users:
        email = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'email'), None)
        username = user['Username']

        if email and email in inactive_emails:
            try:
                print(f"Deleting {email}")
                cognito.admin_delete_user(UserPoolId=USER_POOL_ID, Username=username)
                deleted_emails.append(email)
            except ClientError as e:
                # Log error but continue with other deletions
                print(f"Failed to delete user {username}: {e.response['Error']['Message']}")

    return deleted_emails


def lambda_handler(event, context):
    try:
        # Get the list of valid emails from the event
        active_users = event.get("users", [])

        # Get all users in Cognito
        users = get_all_cognito_users()

        # Delete users not in the active list and get deleted emails
        deleted_emails = delete_unused_cognito_users(users, active_users)

    except ClientError as e:
        error_message = f"Error deleting users from cognito pool: {e.response['Error']['Message']}"
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

    return {
        'statusCode': 200,
        'body': json.dumps({'deleted_emails': deleted_emails})
    }
