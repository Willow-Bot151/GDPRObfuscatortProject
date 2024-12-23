import botocore.session
from src.GDPRObfuscator_handler import *
import pandas as pd
import pyarrow
from io import StringIO, BytesIO
import pytest
import os
from moto import mock_aws
import boto3


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


@pytest.fixture
def clean_test_bucket(mock_s3_client):
    s3_client = mock_s3_client
    test_bucket_name = "test-data-for-obfuscation-bucket"
    test_object_names = ["test_data_csv", "test_data_json", "test_data_parquet"]
    for bucket in s3_client.list_buckets(MaxBuckets=123)["Buckets"]:
        if bucket["Name"] == test_bucket_name:
            if "Contents" in s3_client.list_objects(Bucket=test_bucket_name):
                objects = s3_client.list_objects(Bucket=test_bucket_name)["Contents"]
                for object in objects:
                    object_key = object["Key"]
                    for test_object in test_object_names:
                        if test_object == object_key:
                            s3_client.delete_object(
                                Bucket=test_bucket_name, Key=object_key
                            )
            return True
    s3_client.create_bucket(
        Bucket=test_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )


class TestHandlerIntegration:
    def test_handler_integration_csv(self, clean_test_bucket, mock_s3_client):
        s3_client = mock_s3_client
        test_bucket_name = "test-data-for-obfuscation-bucket"
        test_key_name = "test_data_csv"
        with open("test/test_data/customers-100.csv", "rb") as f:
            response = s3_client.put_object(
                Bucket=test_bucket_name, Key=test_key_name, Body=f
            )
        test_object = f"s3://{test_bucket_name}/{test_key_name}"
        test_event = {
            "s3_path": test_object,
            "obfuscate_fields": ["First Name"],
        }
        test_context = None
        response = lambda_handler(test_event, test_context)
        df = pd.read_csv(StringIO(response.decode("utf-8")), sep=",", header=0)
        assert df["First Name"][0] == "***"

    def test_handler_integration_json(self, clean_test_bucket, mock_s3_client):
        s3_client = mock_s3_client
        test_bucket_name = "test-data-for-obfuscation-bucket"
        test_key_name = "test_data_json"
        with open("test/test_data/json_test_data.json", "rb") as f:
            response = s3_client.put_object(
                Bucket=test_bucket_name, Key=test_key_name, Body=f
            )
        test_object = f"s3://{test_bucket_name}/{test_key_name}"
        test_event = {
            "s3_path": test_object,
            "obfuscate_fields": ["first_name"],
        }
        test_context = None
        response = lambda_handler(test_event, test_context)
        df = pd.DataFrame.from_dict(json.loads(response.decode("utf-8")))
        assert df["first_name"][0] == "***"

    def test_handler_integration_parquet(self, clean_test_bucket, mock_s3_client):
        s3_client = mock_s3_client
        test_bucket_name = "test-data-for-obfuscation-bucket"
        test_key_name = "test_data_parquet"
        with open("test/test_data/parquet_test_data.parquet", "rb") as f:
            response = s3_client.put_object(
                Bucket=test_bucket_name, Key=test_key_name, Body=f
            )
        test_object = f"s3://{test_bucket_name}/{test_key_name}"
        test_event = {
            "s3_path": test_object,
            "obfuscate_fields": ["variety"],
        }
        test_context = None
        response = lambda_handler(test_event, test_context)
        df = pd.read_parquet(BytesIO(response))
        assert df["variety"][0] == "***"
