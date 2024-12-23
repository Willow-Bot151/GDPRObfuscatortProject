import botocore.session
import pytest
import pandas as pd
from src.GDPRObfuscator_handler import *
import boto3
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import os
from io import StringIO, BytesIO
import botocore.errorfactory


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


class TestInitS3Client:
    @pytest.mark.it("Test client connection success")
    @patch("botocore.session.get_session")
    def test_client_connection(self, mocked_session):
        mock_session_rv = mocked_session.return_value
        mock_client = MagicMock()
        mock_session_rv.create_client.return_value = mock_client
        s3_client = init_s3_client()
        mock_session_rv.create_client.assert_called_once_with("s3")
        assert s3_client == mock_client

    @pytest.mark.it("Test client connection failure")
    @patch("botocore.session.get_session")
    def test_client_connection_failure(self, mocked_session):
        mock_session_rv = mocked_session.return_value
        mock_session_rv.create_client.side_effect = ConnectionRefusedError(
            "Connection failed"
        )
        with pytest.raises(ConnectionRefusedError):
            init_s3_client()


class TestGetFile:
    def test_file_path_information_extracted_correctly(self):
        test_file_path_string1 = "s3://my_ingestion_bucket/new_data/file1.csv"
        test_file_path_string2 = "s3://some_bucket/some_file.csv"
        expected_information1 = {
            "bucket_name": "my_ingestion_bucket",
            "key_name": "new_data/file1.csv",
        }
        expected_information2 = {
            "bucket_name": "some_bucket",
            "key_name": "some_file.csv",
        }
        assert get_bucket_and_key_strings(test_file_path_string1) == expected_information1
        assert get_bucket_and_key_strings(test_file_path_string2) == expected_information2

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
        assert json.loads(result) == "testing"


class TestObfuscateData:
    def test_obfuscate_small(self):
        test_csv_string = "student_id,name,course,cohort,graduation_date,email_address\n1234,'John Smith','Software',,'2024-03-31','j.smith@email.com'"
        df = pd.read_csv(StringIO(test_csv_string))
        fields_to_obfuscate = ["name", "email_address"]
        result = produce_obfuscated_data(df=df, pii_fields=fields_to_obfuscate)
        assert (
            result.to_csv(path_or_buf=None, sep=",", header=True, index=False).strip()
            == "student_id,name,course,cohort,graduation_date,email_address\n1234,***,'Software',,'2024-03-31',***"
        )


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
        test_input_bytestream = test_input_string.encode("utf-8")
        csvStringIO = StringIO(test_input_string)
        expected = pd.read_csv(csvStringIO, sep=",", header=0)
        pd.testing.assert_frame_equal(
            convert_bytestream_to_df(
                formatted_bytes=test_input_bytestream, fields=test_input_fields
            )["df"],
            expected,
            check_dtype=True,
        )
        assert (
            convert_bytestream_to_df(
                formatted_bytes=test_input_bytestream, fields=test_input_fields
            )["format"]
            == "csv"
        )

    def test_parquet_to_df(self, make_test_df):
        input_parquet = make_test_df.to_parquet()
        test_input_fields = ["name"]
        expected = make_test_df
        pd.testing.assert_frame_equal(
            convert_bytestream_to_df(
                formatted_bytes=input_parquet, fields=test_input_fields
            )["df"],
            expected,
            check_dtype=True,
        )
        assert (
            convert_bytestream_to_df(
                formatted_bytes=input_parquet, fields=test_input_fields
            )["format"]
            == "parquet"
        )

    def test_json_to_df(self, make_test_df):
        input_json = json.dumps(make_test_df.to_dict(orient="list")).encode("utf-8")
        test_input_fields = ["name"]
        expected = make_test_df
        pd.testing.assert_frame_equal(
            convert_bytestream_to_df(
                formatted_bytes=input_json, fields=test_input_fields
            )["df"],
            expected,
            check_dtype=True,
        )
        assert (
            convert_bytestream_to_df(
                formatted_bytes=input_json, fields=test_input_fields
            )["format"]
            == "json"
        )

    def test_invalid_csv_or_json_formatted_string_raises_error(self):
        test_input_string = """cnhuedsfbuiewfbaflb@::{D1&"&$^$^!£}"""
        with pytest.raises(TypeError):
            convert_bytestream_to_df(test_input_string)

    def test_invalid_parquet_format_raises_error(self):
        test_input_string = """cnhuedsfbuiewfbaflb@::{D1&"&$^$^!£}"""
        test_input_bytestream = test_input_string.encode("utf-8")
        with pytest.raises(TypeError):
            convert_bytestream_to_df(test_input_bytestream)


class TestConvertDfToFormattedString:
    def test_to_string_non_mutation(self, make_test_df):
        test_input = make_test_df.copy()
        test_format = "csv"
        convert_df_to_formatted_bytestream(test_input, test_format)
        pd.testing.assert_frame_equal(test_input, make_test_df, check_dtype=True)

    def test_makes_new_df(self, make_test_df):
        test_input = make_test_df.copy()
        test_format = "csv"
        new_df = convert_df_to_formatted_bytestream(test_input, test_format)
        assert not new_df is test_input

    def test_to_csv_correct(self, make_test_df):
        test_input = make_test_df
        test_format = "csv"
        expected = make_test_df.to_csv(index=False).encode("utf-8")
        assert convert_df_to_formatted_bytestream(test_input, test_format) == expected

    def test_to_parquet_correct(self, make_test_df):
        test_input = make_test_df
        test_format = "parquet"
        expected = make_test_df.to_parquet()
        assert convert_df_to_formatted_bytestream(test_input, test_format) == expected

    def test_to_json_correct(self, make_test_df):
        test_input = make_test_df
        test_format = "json"
        expected = json.dumps(make_test_df.to_dict()).encode("utf-8")
        assert convert_df_to_formatted_bytestream(test_input, test_format) == expected
