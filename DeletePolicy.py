import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    
    try:
        
        iot_client = boto3.client('iot')
        
        # Process the payload variables
        policy_name = event.get("policy_name", "")
        
        
        # Create IoT Core policy
        iot_client.delete_policy(
        policyName=policy_name,
        )
    
    except ClientError as e:
        error_message = f"Error deleting policy: {e.response['Error']['Message']}"
        return {
            'statusCode': 500,
            'body': error_message
        }
        
    return {
        'statusCode': 200,
        'body': 'Policy Deleted Successfully'
    }
    
    
