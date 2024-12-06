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
        result =  produce_obfuscated_data(
            df=df, 
            pii_fields= fields_to_obfuscate
            )
        assert result.to_csv(
            path_or_buf=None,
            sep=',',
            header=True,
            index=False
            ).strip() == "student_id,name,course,cohort,graduation_date,email_address\n1234,'***','Software',,'2024-03-31','***'"

class TestCreateTestBucket:
    def test_create_test_bucket_function_makes_bucket_with_correct_test_data(self,mock_s3_client):
        bucket_name = "testing_bucket"
        test_data_string = "student_id,name,course,cohort,graduation_date,email_address\n1234,'John Smith','Software',,'2024-03-31','j.smith@email.com'"
        test_file_name = "testing_file.csv"
        create_bucket(
            bucket_name=bucket_name,
            data_dict={test_file_name:test_data_string},
            client=mock_s3_client)
        response = mock_s3_client.get_object(
            Bucket=bucket_name,
            Key=test_file_name
        )
        body = response["Body"].read()
        assert json.loads(body.decode("utf-8")) == test_data_string

class TestConvertFormatToDF:
    def test_returns_valid_df(self):
        test_input_string = "name,DoB,fav_colour\nbob,1/1/4000,maroon"
        format = "csv"
        assert isinstance(
            convert_format_to_df(
                formatted_string=test_input_string,
                format=format
            ),
            pd.DataFrame
        ) 
    def test_csv_to_df(self):
        test_input_string = "name,DoB,fav_colour\nbob,1/1/4000,maroon"
        format = "csv"
        csvStringIO = StringIO(test_input_string)
        expected = pd.read_csv(csvStringIO,sep=',',header=None)
        pd.testing.assert_frame_equal(
            convert_format_to_df(
            formatted_string=test_input_string,
            format=format
            ),
            expected,
            check_dtype=True
        ) 
    def test_parquet_to_df(self):
        test_input_string = """
        {
            name: {
                0: Bob,
                1: Steve
            },
            DoB: {
                0: 1/1/4000,
                1: 2/14/0006
            },
            fav_colour: {
                0: maroon,
                1: cheese
            }
        }
        """
        format = "parquet"
        expected = pd.DataFrame({
            "name": ["Bob","Steve"],
            "DoB": ["1/1/4000","2/14/0006"],
            "fav_colour": ["maroon","cheese"]
        })
        pd.testing.assert_frame_equal(
            convert_format_to_df(
                formatted_string=test_input_string,
                format=format
            ),
            expected,
            check_dtype=True
        ) 
    def test_json_to_df(self):
        test_input_string = """
        {
            0: {
                "name":  "Bob",
                "DoB":   "1/1/4000",
                "fav_colour": "maroon"
            },
            1: {
                "name": "Steve",
                "DoB": "2/14/0006",
                "fav_colour": "cheese"
            }
        }
        """
        format = "json"
        expected = pd.DataFrame({
            "name": ["Bob","Steve"],
            "DoB": ["1/1/4000","2/14/0006"],
            "fav_colour": ["maroon","cheese"]
        })
        pd.testing.assert_frame_equal(
            convert_format_to_df(
                formatted_string=test_input_string,
                format=format
            ),
            expected,
            check_dtype=True
        ) 
    def test_invalid_formatted_string_returns_error(self):
        test_input_string = "testing"
        formats = ["csv","json","parquet"]
        for format in formats:
            with pytest.raises(ValueError):
                convert_format_to_df(
                    formatted_string=test_input_string,
                    format=format)
    def test_handle_invalid_format(self):
        test_fake_format = "cheese"
        test_input_string = "name,DoB,fav_colour\nbob,1/1/4000,maroon"
        with pytest.raises(ValueError):
            convert_format_to_df(
                formatted_string=test_input_string,
                format=test_fake_format
            )

class TestFormatValidator:
    def test_identify_csv(self):
        pass
    def test_identify_parquet(self):
        pass
    def test_identify_json(self):
        pass
    def test_invalid_format_errors(self):
        pass

class TestConvertDfToFormattedString:
    def test_to_csv(self):
        pass
    def test_to_parquet(self):
        pass
    def test_to_json(self):
        pass
    def test_invalid_df_raises_exception(self):
        pass

