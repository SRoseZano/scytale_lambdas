import boto3
import logging
import traceback

iot_client = boto3.client('iot')


def register_thing(thing_name, policy_name):
  
    # Register a thing
    response = iot_client.create_thing(
        thingName=thing_name
    )
    
    # Generate certificates for the thing
    certificate_response = iot_client.create_keys_and_certificate(setAsActive=True)
    
    # Extract certificate details
    certificate_arn = certificate_response['certificateArn']
    certificate_id = certificate_response['certificateId']
    certificate_pem = certificate_response['certificatePem']
    private_key = certificate_response['keyPair']['PrivateKey']
    
    # Attach the certificate to the thing
    iot_client.attach_thing_principal(
        thingName=thing_name,
        principal=certificate_arn
    )
    
    iot_client.attach_policy(
        policyName=policy_name,
        target=certificate_arn
    )

    
    # Return the thing name and certificates
    return {
        'thingName': thing_name,
        'certificateId': certificate_id,
        'certificatePem': certificate_pem,
        'privateKey': private_key
    }

def lambda_handler(event, context):
    try:
        thing_name = event.get("thing_name", "")
        policy_name = event.get("policy_name", "")
        # Register a thing and get certificates
        response = register_thing(thing_name, policy_name)
        
        # Return the response
        return {
            'statusCode': 200,
            'body': {
                'thingName': response['thingName'],
                'certificateId': response['certificateId'],
                'certificatePem': response['certificatePem'],
                'privateKey': response['privateKey']
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
