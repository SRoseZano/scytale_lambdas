import boto3
import logging
from datetime import datetime
import stripe
import zanolambdashelper

zanolambdashelper.helpers.set_logging('INFO')

today_midnight = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

stripe_secrets = zanolambdashelper.helpers.get_stripe_api_secrets()

STRIPE_API_KEY_SECRET = stripe_secrets['api_key']

stripe.api_key = STRIPE_API_KEY_SECRET

# Map price IDs to your internal counters AND give each a meter event name
prices = {
    "price_1RqFmm7u40gohPr0ckFt40IX": ("hub_count", "testmonthlymeter"),
    "price_1Rxw737u40gohPr0z4i732Tw": ("emergency_light_count", "emergencylightmeter"),
    "price_1RxvCm7u40gohPr009YD3BJg": ("dimmable_light_count", "lightmeter"),
    "price_1RxvBW7u40gohPr0VPe6GjUB": ("pir_count", "pirmeter"),
    "price_1RyJL57u40gohPr0NwLd1i1q": ("encoder_count", "encodermeter")
}

def lambda_handler(event, context):
    try:
        for sub_id, org_data in event.items():
            logging.info(f"Logging usage for subscription: {sub_id}")

            subscription = stripe.Subscription.retrieve(sub_id)
            customer_id = subscription['customer']

            for item in subscription['items']['data']:
                price_id = item['price']['id']
                mapping = prices.get(price_id)

                if not mapping:
                    continue

                field_name, event_name = mapping
                count = org_data.get(field_name, 0)

                if count <= 0:
                    continue

                logging.info(
                    f"Sending meter event: {event_name}, count={count}, "
                    f"customer={customer_id}, sub_item={item['id']}"
                )

                meter_event = stripe.billing.MeterEvent.create(
                    event_name=event_name,
                    identifier=f"{item['id']}_{today_midnight}",  # unique per subscription item
                    payload={
                        "value": str(count),
                        "stripe_customer_id": customer_id
                    }
                )

                logging.info(f"Meter event created: {meter_event}")

    except Exception as e:
        logging.error(f"Error updating stripe subscriptions: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f"Error updating stripe subscriptions: {e}"
        }

    return {
        'statusCode': 200,
        'body': 'Stripe meter events logged successfully'
    }
