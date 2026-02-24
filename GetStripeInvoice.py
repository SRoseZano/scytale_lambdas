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

        stripe_invoice_id = event.get("stripe_invoice_id")

        if not stripe_invoice_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "stripe_invoice_id is required"})
            }

        # Retrieve invoice from Stripe
        invoice = stripe.Invoice.retrieve(stripe_invoice_id)

        # pdf url
        pdf_url = invoice.invoice_pdf

        if not pdf_url:
            return {
                "statusCode": 404,
                "body": "Invoice URL not available"
            }

        return {
            "statusCode": 200,
            "body": "Invoice URL retrieved",
            "url": pdf_url

        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }