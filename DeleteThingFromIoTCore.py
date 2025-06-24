import json
import boto3
from botocore.exceptions import ClientError

iot = boto3.client('iot')

def get_all_things():
    things = []
    paginator = iot.get_paginator('list_things')
    for page in paginator.paginate():
        things.extend(page['things'])
    return things

def detach_policies_and_principals(thing_name):
    try:
        response = iot.list_thing_principals(thingName=thing_name)
        principals = response.get('principals', [])

        for principal in principals:
            # Detach policies from the principal
            policies = iot.list_attached_policies(target=principal).get('policies', [])
            for policy in policies:
                print(f"Detaching policy {policy['policyName']} from {principal}")
                iot.detach_policy(policyName=policy['policyName'], target=principal)

            # Detach the principal from the thing
            print(f"Detaching principal {principal} from thing {thing_name}")
            iot.detach_thing_principal(thingName=thing_name, principal=principal)

    except ClientError as e:
        print(f"Failed to detach policies/principals from {thing_name}: {e}")
        raise

def clean_iot_things(iot_things, db_hub_uuids):
    batch = []
    for thing in iot_things:
        if thing['thingName'] not in db_hub_uuids:
            batch.append(thing['thingName'])

    for thing_name in batch:
        print(f"Cleaning up thing: {thing_name}")
        try:
            detach_policies_and_principals(thing_name)

            # Now delete the thing
            print(f"Deleting thing {thing_name}")
            iot.delete_thing(thingName=thing_name)

        except Exception as e:
            print(f"Failed to delete {thing_name}: {e}")
            raise  # re-raise or handle accordingly

def lambda_handler(event, context):
    try:
        # Process the payload variables
        active_things = event.get("things", [])

        things = get_all_things()
        print(things)
        print(active_things)

        clean_iot_things(things, active_things)

    except Exception as e:
        error_message = f"Error deleting things from IoT Core: {e}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': error_message
        }

    return {
        'statusCode': 200,
        'body': 'Things deleted successfully'
    }
