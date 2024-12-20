import json
import botocore.client
import pandas as pd
import botocore
from io import StringIO, BytesIO


def lambda_handler(event, context):
    print(event)
    s3_path = event["s3_path"] 
    fields = event["obfuscate_fields"]
    s3_client = init_s3_client()
    path_elements = get_bucket_and_key_strings(s3_path)
    file = get_file_from_bucket(
        bucket_name=path_elements["bucket_name"],
        file_name=path_elements["key_name"],
        client=s3_client,
    )
    df_dict = convert_string_to_df(file, fields)
    df = df_dict["df"]
    found_format = df_dict["format"]
    new_df = produce_obfuscated_data(df, fields)
    return convert_df_to_formatted_string(new_df, found_format)

    """
    retrieve file
    assert file format
    convert to dataframe
    change fields
    convert to found file format
    return
    """


def lambda_event(path, fields):
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
    lambda_client = botocore.client("lambda", region_name="eu-west-2")

    payload = {"s3_path": path, "obfuscate_fields": fields}

    response = lambda_client.invoke(
        FunctionName="lambda_handler",
        InvokationType="RequestResponse",
        Payload=json.dumps(payload),
    )


import json
import botocore.client
import pandas as pd
import botocore
from io import StringIO, BytesIO


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
    response = client.get_object(Bucket=bucket_name, Key=file_name)
    body = response["Body"].read()
    file = json.loads(body.decode("utf-8"))
    return file


def get_bucket_and_key_strings(file_path):
    path_elements = file_path.split("/")
    bucket_name = path_elements[2]
    key_name = ""
    for ele in path_elements[3:]:
        key_name += "/" + ele
    return {"bucket_name": bucket_name, "key_name": key_name}


def produce_obfuscated_data(df, pii_fields):
    new_df = df.copy()
    new_df[pii_fields] = "'***'"
    return new_df


def create_bucket(bucket_name, data_dict, client):
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
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    print(data_dict)
    for k in data_dict:
        client.put_object(Bucket=bucket_name, Key=k, Body=json.dumps(data_dict[k]))


def convert_string_to_df(formatted_string, fields):
    """
    This function will identify the format convention of a string representing a dataset.
    It will work if the format is valid csv, json or parquet.

    Parameters:
        - formatted_string
            A string or bytes formatted with csv, json, or parquet.
        - fields
            The list of fields to obfuscate. Used as a tool to validate the format of the string
    Return:
        - A dictionary containing the fields:
            - df
                the data frame converted from the input
            - format
                the format of the input as identified by this function
    """
    df_dict = dict()
    if isinstance(formatted_string, str):
        stringio = StringIO(formatted_string)
        try:
            df = pd.read_csv(stringio, sep=",", header=0)
            if set(fields).issubset(set(df.columns.values.tolist())):
                df_dict["format"] = "csv"
            else:
                df = pd.DataFrame.from_dict(json.loads(formatted_string))
                if set(fields).issubset(set(df.columns.values.tolist())):
                    df_dict["format"] = "json"
                else:
                    raise TypeError(
                        "Failed to interpret string as json or csv"
                    )  ## raise error, format probably wrong
        except TypeError as e:
            raise e
        except Exception as e:
            raise e
    elif isinstance(formatted_string, bytes):
        bytesio = BytesIO(formatted_string)
        try:
            df = pd.read_parquet(bytesio)
            if set(fields).issubset(set(df.columns.values.tolist())):
                df_dict["format"] = "parquet"
            else:
                raise TypeError("Failed to interpret bytes as parquet")
        except TypeError as e:
            raise e
        except Exception as e:
            raise e
    else:
        raise TypeError("expected string or bytes type for formatted_string")
    df_dict["df"] = df
    return df_dict


def convert_df_to_formatted_string(df, format):
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
    if format == "csv":
        try:
            return df.to_csv()
        except Exception as e:
            raise e
    elif format == "json":
        try:
            return json.dumps(df.to_dict())
        except Exception as e:
            raise e
    elif format == "parquet":
        try:
            return df.to_parquet()
        except Exception as e:
            raise e
