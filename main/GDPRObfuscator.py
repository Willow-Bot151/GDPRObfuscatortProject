import json
import pandas as pd
import botocore

def Obfuscator(JSON_string):
    input_info = json.loads(JSON_string)
    print(input_info)
    target_file_path, fields_to_obfuscate = input_info["file_to_obfuscate"], input_info['pii_fields']
    print(target_file_path, fields_to_obfuscate)
    ## retrieve target file with boto3
    client = init_s3_client()
    get_bucket_and_key_strings(target_file_path)
    csv_file_path = "temp/customers-100.csv"
    df = pd.read_csv(csv_file_path)
    # print(df)
    return df.to_csv()

def init_s3_client():
    """
    Initialises an s3 client using boto3.

            Parameters:
                    No inputs are taken for this function.

            Returns:
                    An instance of s3 client.
    """
    try:
        session = botocore.session.get_session()
        s3_client = session.create_client("s3")
        return s3_client

    except Exception:
        raise ConnectionRefusedError("Failed to connect to s3 client")

def get_file_from_bucket(path, client):
    pass

def get_bucket_and_key_strings(file_path):
    print(file_path.split('/'))
    