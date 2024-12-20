import pytest
import pandas as pd
from src.GDPRObfuscator_handler import *
import boto3
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import os
from io import StringIO, BytesIO

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


# def test_json_input():
#     test_input = '{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv","pii_fields": ["name", "email_address"]}'
#     Obfuscator(test_input)


# def test_returns_csv():
#     test_input = '{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv","pii_fields": ["name", "email_address"]}'
#     output = Obfuscator(test_input)


class TestGetFile:
    def test_file_path_information_extracted_correctly(self):
        test_file_path_string = "s3://my_ingestion_bucket/new_data/file1.csv"
        expected_information = {
            "bucket_name": "my_ingestion_bucket",
            "key_name": "/new_data/file1.csv",
        }
        assert get_bucket_and_key_strings(test_file_path_string) == expected_information

    def test_get_file(self, mock_s3_client):
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
            bucket_name=test_bucket_name, file_name=test_key_name, client=mock_s3_client
        )
        assert result == "testing"


class TestObfuscateData:
    def test_obfuscate_small(self):
        test_csv_string = "student_id,name,course,cohort,graduation_date,email_address\n1234,'John Smith','Software',,'2024-03-31','j.smith@email.com'"
        df = pd.read_csv(StringIO(test_csv_string))
        fields_to_obfuscate = ["name", "email_address"]
        result = produce_obfuscated_data(df=df, pii_fields=fields_to_obfuscate)
        assert (
            result.to_csv(path_or_buf=None, sep=",", header=True, index=False).strip()
            == "student_id,name,course,cohort,graduation_date,email_address\n1234,'***','Software',,'2024-03-31','***'"
        )


class TestCreateTestBucket:
    def test_create_test_bucket_function_makes_bucket_with_correct_test_data(
        self, mock_s3_client
    ):
        bucket_name = "testing_bucket"
        test_data_string = "student_id,name,course,cohort,graduation_date,email_address\n1234,'John Smith','Software',,'2024-03-31','j.smith@email.com'"
        test_file_name = "testing_file.csv"
        create_bucket(
            bucket_name=bucket_name,
            data_dict={test_file_name: test_data_string},
            client=mock_s3_client,
        )
        response = mock_s3_client.get_object(Bucket=bucket_name, Key=test_file_name)
        body = response["Body"].read()
        assert json.loads(body.decode("utf-8")) == test_data_string


@pytest.fixture
def make_test_df():
    return pd.DataFrame(
        {
            "name": ["Bob", "Steve"],
            "DoB": ["1/1/4000", "2/14/0006"],
            "fav_colour": ["maroon", "cheese"],
        }
    )


class TestConvertFormatToDF:
    def test_csv_to_df(self):
        test_input_string = "name,DoB,fav_colour\nbob,1/1/4000,maroon"
        test_input_fields = ["name"]
        csvStringIO = StringIO(test_input_string)
        expected = pd.read_csv(csvStringIO, sep=",", header=0)
        pd.testing.assert_frame_equal(
            convert_string_to_df(
                formatted_string=test_input_string, fields=test_input_fields
            )["df"],
            expected,
            check_dtype=True,
        )
        assert (
            convert_string_to_df(
                formatted_string=test_input_string, fields=test_input_fields
            )["format"]
            == "csv"
        )

    def test_parquet_to_df(self, make_test_df):
        input_parquet = make_test_df.to_parquet()
        test_input_fields = ["name"]
        expected = make_test_df
        pd.testing.assert_frame_equal(
            convert_string_to_df(
                formatted_string=input_parquet, fields=test_input_fields
            )["df"],
            expected,
            check_dtype=True,
        )
        assert (
            convert_string_to_df(
                formatted_string=input_parquet, fields=test_input_fields
            )["format"]
            == "parquet"
        )

    def test_json_to_df(self, make_test_df):
        input_json = json.dumps(make_test_df.to_dict(orient="list"))
        test_input_fields = ["name"]
        expected = make_test_df
        pd.testing.assert_frame_equal(
            convert_string_to_df(formatted_string=input_json, fields=test_input_fields)[
                "df"
            ],
            expected,
            check_dtype=True,
        )
        assert (
            convert_string_to_df(formatted_string=input_json, fields=test_input_fields)[
                "format"
            ]
            == "json"
        )

    def test_invalid_csv_or_json_formatted_string_raises_error(self):
        test_input_string = """cnhuedsfbuiewfbaflb@::{D1&"&$^$^!£}"""
        with pytest.raises(TypeError):
            convert_string_to_df(test_input_string)

    def test_invalid_parquet_format_raises_error(self):
        test_input_string = """cnhuedsfbuiewfbaflb@::{D1&"&$^$^!£}"""
        test_input_bytestream = test_input_string.encode("utf-8")
        with pytest.raises(TypeError):
            convert_string_to_df(test_input_bytestream)


class TestConvertDfToFormattedString:
    def test_to_string_non_mutation(self, make_test_df):
        test_input = make_test_df.copy()
        test_format = "csv"
        convert_df_to_formatted_string(test_input, test_format)
        pd.testing.assert_frame_equal(test_input, make_test_df, check_dtype=True)

    def test_makes_new_df(self, make_test_df):
        test_input = make_test_df.copy()
        test_format = "csv"
        new_df = convert_df_to_formatted_string(test_input, test_format)
        assert not new_df is test_input

    def test_to_csv_correct(self, make_test_df):
        test_input = make_test_df
        test_format = "csv"
        expected = make_test_df.to_csv()
        assert convert_df_to_formatted_string(test_input, test_format) == expected

    def test_to_parquet_correct(self, make_test_df):
        test_input = make_test_df
        test_format = "parquet"
        expected = make_test_df.to_parquet()
        assert convert_df_to_formatted_string(test_input, test_format) == expected

    def test_to_json_correct(self, make_test_df):
        test_input = make_test_df
        test_format = "json"
        expected = json.dumps(make_test_df.to_dict())
        assert convert_df_to_formatted_string(test_input, test_format) == expected

    def test_invalid_df_raises_exception(self):
        pass

@pytest.fixture
def mock_bucket(mock_s3_client):
    pass

class TestHandlerUnitTests:
    """
    mock bucket
    fake data
    
    """
    def test():
        pass

class TestHandlerIntegrationTests:
    def test(self):
        pass