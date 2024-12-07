import json
import pandas as pd
import botocore
from io import StringIO

def Obfuscator(JSON_string):
    input_info = json.loads(JSON_string)
    target_file_path, fields_to_obfuscate = input_info["file_to_obfuscate"], input_info['pii_fields']
    ## retrieve target file with boto3
    client = init_s3_client()
    get_bucket_and_key_strings(target_file_path)
    csv_file_path = "temp/customers-100.csv"
    df = pd.read_csv(csv_file_path)
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

def get_file_from_bucket(bucket_name, file_name, client):
    """
    Gets specified file from bucket.

            Parameters:
                    Requires a boto3 s3 client connection.
                    Requires a path string to the object

            Returns:
                    The target file csv.
    """
    response = client.get_object(
        Bucket=bucket_name, 
        Key=file_name)
    body = response["Body"].read()
    file = json.loads(body.decode("utf-8"))
    return file

def get_bucket_and_key_strings(file_path):
    path_elements = file_path.split('/')
    bucket_name = path_elements[2]
    key_name = ""
    for ele in path_elements[3:]:
        key_name += "/" + ele
    return {"bucket_name":bucket_name, "key_name":key_name}
    
def produce_obfuscated_data(df, pii_fields):
    new_df = df.copy()
    new_df[pii_fields] = "'***'"
    return new_df

def create_bucket(bucket_name,data_dict,client):
    """
    This function creates an s3 bucket.

    Parameters:
        - test_bucket_name 
            Name of bucket to be created.
        - test_data_string_dict 
            Dictionary of test data,.
            The key of each element should be the file name of the data set.
            The data should be in CSV string, JSON string, or Parquet string format.
    Returns:
        - None
    """
    client.create_bucket(
        Bucket = bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint":"eu-west-2"
        }
    )
    print(data_dict)
    for k in data_dict:
        client.put_object(
            Bucket = bucket_name,
            Key = k,
            Body = json.dumps(data_dict[k])
        )

def convert_format_to_df(formatted_string,format):
    """
    This function will convert a json, csv, or parquet format string to a dataframe.

    Parameters:
        - formatted_string
            A string in csv, json or parquet format.
            An expression of a data table.
        - format
            "csv", "json" or "parquet"
            The format of the string.
    Returns:
        - A pandas dataframe containing the dataset from the input data table.
    """
    if format == "csv":
        try:
            csv_string = StringIO(formatted_string)
            df = pd.read_csv(csv_string,sep=',',header=None)
            return df
        except Exception as e:
            raise Exception("failed to convert csv string to dataframe, please ensure string is valid csv.") from e

    elif format == "json":
        try:
            pass
        except:
            pass
    elif format == "parquet":
        try:
            pass
        except:
            pass
    else:
        raise ValueError("format arguement is invalid, format can be \"csv\",\"json\" or \"parquet\"")

def format_validator(formatted_string):
    """
    This function will identify the format convention of a string representing a dataset.
    It will work if the format is valid csv, json or parquet.

    Parameters:
        - formatted_string
            A string formatted with csv, json, or parquet.
    Return:
        - A string representing the format of the input formatted string.
        - Will be "csv", "json" or "parquet"
    """
    pass

def convert_df_to_formatted_string(df,format):
    """
    This function will convert a dataframe to a string of a desired format.

    Parameters:
        - df
            A dataframe of the dataset to convert into a formatted string.
        - format
            The desired format of the string.
            Must be "csv", "json" or "parquet"

    Returns:
        - A formatted string convertion of the dataframe. 
    """
    pass