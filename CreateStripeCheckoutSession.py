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
        org_uuid = body_json.get("org_uuid")

        if not org_uuid:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "org_uuid is required"})
            }

        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[
                {"price": "price_1RqFmm7u40gohPr0ckFt40IX"},
                {"price": "price_1Rxw737u40gohPr0z4i732Tw"},
                {"price": "price_1RxvCm7u40gohPr009YD3BJg"},
                {"price": "price_1RxvBW7u40gohPr0VPe6GjUB"},
                {"price": "price_1RyJL57u40gohPr0NwLd1i1q"},
            ],
            success_url="myapp://checkout-success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="myapp://checkout-cancel",
            subscription_data={"metadata": {"org_uuid": org_uuid}},
        )

        return {
            'statusCode': 200,
            'body': 'Device Added Successfully',
            'url': session.url,
            'sessionId': session.id
        }


    except stripe.error.StripeError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)}),
        }
