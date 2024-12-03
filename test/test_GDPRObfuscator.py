import pytest
import pandas as pd
from main.GDPRObfuscator import *
import boto3

"""
handle data in CSV
handle data in JSON
handle data in parquet
access supplied file location in s3 bucket
be supplied affected fields
create and return file or byte-stream object
invoked with JSON string
"""

def test_json_input():
    test_input = '{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv","pii_fields": ["name", "email_address"]}'
    Obfuscator(test_input)

def test_returns_csv():
    test_input = '{"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv","pii_fields": ["name", "email_address"]}'
    output =  Obfuscator(test_input)
    print(output)
