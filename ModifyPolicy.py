import json
import boto3

def lambda_handler(event, context):
    
    try:
        
        iot_client = boto3.client('iot')
        
        # Process the payload variables
        policy_name = event.get("policy_name", "")
        device_id = event.get("device_id","")
        
        policy_name = 'Policy_9a00e232-b47a-11ee-b737-0217177d694b'
        device_id = '00-D4-D0-13-X2-26'
        
        print(policy_name, device_id)
        
    
        resource = f"arn:aws:iot:eu-west-2:252856254277:topic/{device_id}"
    
        # Retrieve the existing IoT policy document
        response = iot_client.get_policy(policyName=policy_name)
        policy_document = json.loads(response['policyDocument'])
    
        # Check if the policy has a statement with all expected permissions
        expected_permissions = ['iot:Publish', 'iot:Subscribe', 'iot:Receive']
        found = False
        for statement in policy_document['Statement']:
            if all(permission in statement['Action'] for permission in expected_permissions):
                found = True
                # Check if 'Resource' is already a list, if not, make it a list
                if 'Resource' not in statement:
                    statement['Resource'] = []
                # Add the additional resource to the existing list
                statement['Resource'].append(resource)
    
        # If the statement is not found, add a new statement with all required permissions and resource
        if not found:
            new_statement = {
                'Effect': 'Allow',
                'Action': expected_permissions,
                'Resource': [resource]
            }
            policy_document['Statement'].append(new_statement)
    
        # Convert the modified policy document back to JSON
        updated_policy_document = json.dumps(policy_document)
        
        
        # Update the IoT policy with the modified document
        iot_client.create_policy_version(
                        policyName=policy_name,
                        policyDocument=updated_policy_document,
                        setAsDefault=True
                        )
        
        # List all policy versions
        list_versions_response = iot_client.list_policy_versions(
            policyName=policy_name
        )
        
        """
        
        # Get the latest versionId from the list
        latest_version = max(list_versions_response['policyVersions'], key=lambda x: x['versionId'])
        max_versions_to_keep = 1
    
        # Iterate through versions and delete all versions except the latest ones
        for version in list_versions_response['policyVersions']:
            print(version)
            if int(version['versionId']) < int(latest_version['versionId']) - max_versions_to_keep:
                iot_client.delete_policy_version(
                    PolicyArn=f'arn:aws:iot:eu-west-2:252856254277:policy/{policy_name}',
                    VersionId=36
                )
                    
        """
        
    except Exception as e:
        return {
                'statusCode': 400,
                'error': e
        }
        
    return {
        'statusCode': 200,
        'body': 'Policy Modified Successfully'
    }
