import json
import stripe
import boto3
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

stripe_secrets = zanolambdashelper.helpers.get_stripe_api_secrets()

STRIPE_API_SECRET = stripe_secrets['api_key']

stripe.api_key = STRIPE_API_SECRET

def lambda_handler(event, context):
    try:
        body_json = event['body-json']
        session_id = body_json.get("session_id")

        if not session_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "session_id is required"})
            }

        session = stripe.checkout.Session.retrieve(session_id)

        if not session.subscription:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "No subscription found for this session"})
            }

        subscription = stripe.Subscription.retrieve(session.subscription)

        return {
            "statusCode": 200,
            "body": "Subscription found",
            "subscriptionID":
                json.dumps({"subscriptionId": subscription.id}),
        }

    except stripe.error.StripeError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)}),
        }
