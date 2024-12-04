import pytest
import pandas as pd
from main.GDPRObfuscator import *
import boto3
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import os
from io import StringIO

"""
handle data in CSV
handle data in JSON
handle data in parquet
access supplied file location in s3 bucket
be supplied affected fields
create and return file or byte-stream object
invoked with JSON string
"""

@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture
def mock_s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")

def test_json_input():
    test_input = '{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv","pii_fields": ["name", "email_address"]}'
    Obfuscator(test_input)

def test_returns_csv():
    test_input = '{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv","pii_fields": ["name", "email_address"]}'
    output =  Obfuscator(test_input)
    print(output)

class TestGetFile:
    def test_file_path_information_extracted_correctly(self):
        test_file_path_string = "s3://my_ingestion_bucket/new_data/file1.csv"
        expected_information = {
            "bucket_name":"my_ingestion_bucket",
            "key_name":"/new_data/file1.csv"}
        assert get_bucket_and_key_strings(test_file_path_string) == expected_information
    def test_get_file(self,mock_s3_client):
        test_bucket_name = "test_bucket_bababa"
        test_key_name = "some_test_file"
        mock_s3_client.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_s3_client.put_object(
            Bucket=test_bucket_name,
            Key=test_key_name,
            Body=json.dumps("testing"),
        )
        result = get_file_from_bucket(
            bucket_name=test_bucket_name,
            file_name=test_key_name,
            client=mock_s3_client)
        assert result == "testing"

class TestObfuscateData:
    def test_obfuscate_small(self):
        test_csv_string = "student_id,name,course,cohort,graduation_date,email_address\n1234,'John Smith','Software',,'2024-03-31','j.smith@email.com'"
        df = pd.read_csv(StringIO(test_csv_string))
        fields_to_obfuscate = ["name","email_address"]
        assert produce_obfuscated_data(
            df=df, 
            pii_fields= fields_to_obfuscate
            ) == "student_id,name,course,cohort,graduation_date,email_address\n1234,'***','Software',,'2024-03-31','***'"