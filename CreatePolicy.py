import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    
    try:
        
        iot_client = boto3.client('iot')
        
        # Process the payload variables
        policy_name = event.get("policy_name", "")
        organisation_UUID = event.get("organisation_UUID","")
        
        pub_resource = f"arn:aws:iot:eu-west-2:252856254277:topic/{organisation_UUID}/*"
        sub_resource = f"arn:aws:iot:eu-west-2:252856254277:topicfilter/{organisation_UUID}/*"
        
        # Define the policy document
        # Define the corrected policy document
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "iot:Connect",
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "iot:Publish",
                        "iot:Receive",
                        "iot:RetainPublish",
                        "iot:ListRetainedMessages",
                        "iot:GetRetainedMessage"
                        
                    ],
                    "Resource": [
                        pub_resource
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "iot:Subscribe"
                    ],
                    "Resource": [
                        sub_resource
                    ]
                }
            ]
        }
        
        print(policy_document)
        
        policy_document_json = json.dumps(policy_document)
        
        # Create IoT Core policy
        iot_client.create_policy(
        policyName=policy_name,
        policyDocument=policy_document_json
        )
    
    except ClientError as e:
        error_message = f"Error creating policy: {e.response['Error']['Message']}"
        return {
            'statusCode': 500,
            'body': error_message
        }
        
    return {
        'statusCode': 200,
        'body': 'Policy Created Successfully'
    }
    
    
