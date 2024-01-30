import boto3
import requests
import os

# AWS S3 credentials and bucket information
AWS_ACCESS_KEY = 'your_access_key'
AWS_SECRET_KEY = 'your_secret_key'
S3_BUCKET_NAME = 'airtable-images'

# Salesforce credentials and endpoint
SALESFORCE_ENDPOINT = 'https://your-salesforce-instance.salesforce.com'
SALESFORCE_ACCESS_TOKEN = 'your_access_token'

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# List objects in the S3 bucket
objects = s3.list_objects(Bucket=S3_BUCKET_NAME)

for obj in objects.get('Contents', []):
    # Get the Salesforce Opportunity ID from custom metadata
    salesforce_id = obj.get('Metadata', {}).get('x-amz-salesforceID')
    
    if salesforce_id:
        # Create a temporary download path
        download_path = '/tmp/' + obj['Key']
        
        # Download the S3 object to the local file
        s3.download_file(S3_BUCKET_NAME, obj['Key'], download_path)
        
        # Create an HTTP request to upload the file as a Salesforce Attachment
        headers = {
            'Authorization': f'Bearer {SALESFORCE_ACCESS_TOKEN}',
        }
        files = {'Body': (obj['Key'], open(download_path, 'rb')}
        params = {
            'parentId': salesforce_id,
        }
        
        # Make a POST request to create the Attachment
        response = requests.post(
            f'{SALESFORCE_ENDPOINT}/services/data/v53.0/sobjects/Attachment/',
            headers=headers,
            params=params,
            files=files
        )
        
        # Check if the request was successful
        if response.status_code == 201:
            print(f'Successfully attached {obj["Key"]} to Opportunity {salesforce_id}')
        else:
            print(f'Failed to attach {obj["Key"]} to Opportunity {salesforce_id}')
        
        # Clean up by deleting the local file
        os.remove(download_path)
