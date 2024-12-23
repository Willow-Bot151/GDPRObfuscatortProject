import json
import botocore.client
import botocore.session
import pandas as pd
import time
from src.GDPRObfuscator_handler import lambda_handler

def demonstration():
    session = botocore.session.get_session()
    s3_client = session.create_client("s3",region_name="eu-west-2")
    bucket_name = 'obfuscation-demonstration-bucket'
    key_name = 'demonstration-data.csv'
    s3_client.create_bucket(
        Bucket = bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )
    with open("test/test_data/customers-100.csv", "rb") as f:
        print("s3 file to obfuscate:\n",f.read().decode('utf-8'))
        s3_client.put_object(
            Bucket=bucket_name, Key=key_name, Body=f
        )
    return {
        "s3_path" : f"s3://{bucket_name}/{key_name}",
        "obfuscate_fields" : ["First Name","Last Name"]
    }

def demonstration_clean_up():
    bucket_name = 'obfuscation-demonstration-bucket'
    key_name = 'demonstration-data.csv'
    session = botocore.session.get_session()
    s3_client = session.create_client("s3",region_name="eu-west-2")
    s3_client.delete_object(
        Bucket=bucket_name,
        Key=key_name
    )
    s3_client.delete_bucket(
        Bucket=bucket_name
    )

def lambda_event(event_json):
    """
    This function will format a lambda event to trigger the deployed lambda handler function.
    Parameters:
        - path
            The s3 file path of the data set to be obfuscated.
        - fields
            The column names of the areas of the data set to be obfuscated.
    Returns:
        - nothing
    """
    lambda_client = boto3.client("lambda", region_name="eu-west-2") 
    payload = event_json
    response = lambda_client.invoke(
        FunctionName="Obfuscator_lambda",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )
    return response['Payload'].read().decode('utf-8')

demonstration_clean_up()
event_json = demonstration()


### CALL LAMBDA HANDLER LOCALLY TO OBFUSCATE FILES WITHOUT DEPLOYING THE LAMBDA ###
response = lambda_handler(event_json,None)
print("file obfuscated locally:\n",response)


### CALL LAMBDA HANDLER DEPLOYED AS LAMBDA ###
time.sleep(30)
response = lambda_event(event_json)
print("file obfuscated by deployed lambda:\n",response)


demonstration_clean_up()