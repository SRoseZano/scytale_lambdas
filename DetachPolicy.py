import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Create IoT client
    iot_client = boto3.client('iot')
    
    # Process the payload variables
    policy_name = event.get("policy_name", "")
    user_identity = event.get("user_identity", "")
    

    try:
        # Attach the policy to the principal
        iot_client.detach_principal_policy(
            policyName=policy_name,
            principal=user_identity
        )
        return {
            'statusCode': 200,
            'body': 'Policy detached successfully.'
        }
    except ClientError as e:
        error_message = f"Error attaching policy to principal: {e.response['Error']['Message']}"
        return {
            'statusCode': 500,
            'body': error_message
        }
