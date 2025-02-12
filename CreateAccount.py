import boto3
import logging
import traceback
import random
import string

cognitoPoolId = 'eu-west-2_S9timSWu1'
client = boto3.client('cognito-idp')


def generate_random_username(length=10):
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return username + '@zanocontrols.co.uk'


def generate_random_password(length=12):
    # Define characters for each category
    numbers = string.digits
    special_chars = '!?*#'
    uppercase_letters = string.ascii_uppercase
    lowercase_letters = string.ascii_lowercase

    # Ensure at least one character from each category
    password = random.choice(numbers) + random.choice(special_chars) + random.choice(uppercase_letters) + random.choice(
        lowercase_letters)

    # Add random characters to fill up to the desired length
    password += ''.join(random.choices(numbers + special_chars + uppercase_letters + lowercase_letters, k=length - 4))

    # Shuffle the password characters to ensure randomness
    password_list = list(password)
    random.shuffle(password_list)
    password = ''.join(password_list)

    print(password)

    return password


def create_cognito_user(username, password):
    response = client.admin_create_user(
        UserPoolId=cognitoPoolId,
        Username=username,
        MessageAction='SUPPRESS',  # Suppress email confirmation
        UserAttributes=[
            {
                'Name': 'email',
                'Value': username
            },
            {
                'Name': 'birthdate',
                'Value': '01/01/1990'
            },
            {
                'Name': 'zoneinfo',
                'Value': 'EN'
            },
            {
                'Name': 'locale',
                'Value': 'EN'
            },
            {
                'Name': 'custom:first_name',
                'Value': 'John'
            },
            {
                'Name': 'custom:last_name',
                'Value': 'Doe'
            },
            {
                'Name': 'email_verified',
                'Value': 'true'
            }
        ],
        TemporaryPassword=password,
        DesiredDeliveryMediums=[
            'EMAIL'
        ]
    )


def set_user_password(username, password):
    response = client.admin_set_user_password(
        UserPoolId=cognitoPoolId,
        Username=username,
        Password=password,
        Permanent=True
    )


def lambda_handler(event, context):
    try:
        username = generate_random_username()
        password = generate_random_password()
        create_cognito_user(username, password)
        set_user_password(username, password)

        return {
            'statusCode': 200,
            'body': {
                'username': username,
                'password': password
            }
        }
    except Exception as e:
        logging.error(f"Internal Server Error: {e}")
        error_status_code = e.args[1] if len(e.args) > 1 else 500
        error_body = e.args[0] if len(e.args) > 0 else f"Unspecified error: {e}"
        error_response = {
            'statusCode': error_status_code,
            'body': {'message': error_body}
        }
        return error_response


