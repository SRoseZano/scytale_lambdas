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

        stripe_sub_id = event.get("stripe_sub_id")

        if not stripe_sub_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "stripe_sub_id is required"})
            }
        print(stripe_sub_id)
        invoices = stripe.Invoice.list(subscription=stripe_sub_id, limit=36)
        result = {}

        for invoice in invoices.data:
            date = datetime.fromtimestamp(invoice.created)
            year = date.strftime("%Y")
            month = date.strftime("%b")

            result.setdefault(year, {})[month] = invoice.id

        print(result)

        return {
            'statusCode': 200,
            'body': 'Invoices Returned Successfully',
            'invoices': result,
        }

    except stripe.error.StripeError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)}),
        }
