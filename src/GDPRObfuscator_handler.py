import json
import botocore.client
import pandas as pd
import botocore.session
from io import StringIO, BytesIO


def lambda_handler(event, context):
    """
    This function is the handler for the obfuscator tool, 
    requests to obfuscate s3 data are passed to this handler.
    Parameters:
        - When used locally:
            - event must be a json containing information on the obfuscation 
            task given to the handler. e.g.
            event = {
                "s3_path" : "s3://my_bucket/my_file_key",
                "obfuscate_fields" : ["sensitive data field1", "sensitive data fieldn"]
            }
            - context is not used and can be passed context = None
        - When deployed using terraform:
        the handler is called from aws lambda. 
        e.g. using boto3 the event must be included in the payload of the lambda invocation:
            lambda_client = boto3.client("lambda")
            lambda_client.invoke(
                FunctionName="Obfuscator_lambda",
                InvocationType="RequestResponse",
                Payload=json.dumps(
                    {
                        "s3_path" : "s3://my_bucket/my_file_key",
                        "obfuscate_fields" : ["sensitive data field1", "sensitive data fieldn"]
                    }
                )
            )
    Returns:
        - A bytestream of the data from the obfuscated file 
        with sensitive data replaced by ***.
        The object is ready and compatible for a boto3.put_object operation.
            
    """
    s3_path = event["s3_path"]
    fields = event["obfuscate_fields"]
    s3_client = init_s3_client()
    path_elements = get_bucket_and_key_strings(s3_path)
    file = get_file_from_bucket(
        bucket_name=path_elements["bucket_name"],
        file_name=path_elements["key_name"],
        client=s3_client,
    )
    df_dict = convert_bytestream_to_df(file, fields)
    df = df_dict["df"]
    found_format = df_dict["format"]
    new_df = produce_obfuscated_data(df, fields)
    return convert_df_to_formatted_bytestream(new_df, found_format)


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
    except Exception as e:
        raise e


def get_file_from_bucket(bucket_name, file_name, client):
    """
    Gets specified file from bucket.

            Parameters:
                    Requires a boto3 s3 client connection.
                    Requires a string naming the object's bucket
                    Requires a string naming the object's key 

            Returns:
                    The target file.
    """
    response = client.get_object(Bucket=bucket_name, Key=file_name)
    body = response["Body"].read()
    return body


def get_bucket_and_key_strings(file_path):
    """
    From an s3 path in the format "s3://bucket/key" this function will
    extract the path information (bucket and key)
    Parameters:
        - takes the file path in format "s3://bucket/key"

    Returns:
        - a dictionary in format:
        {
            "bucket_name":bucket,
            "key_name":key
        }
    """
    path_elements = file_path.split("/")
    bucket_name = path_elements[2]
    key_name = ""
    first_key_ele_toggle = False
    for ele in path_elements[3:]:
        if first_key_ele_toggle == True:
            key_name += "/"
        key_name += ele
        first_key_ele_toggle = True
    return {"bucket_name": bucket_name, "key_name": key_name}


def produce_obfuscated_data(df, pii_fields):
    """
    This function will replace specified fields of a dataframe with
    obfuscated strings "***"
    Parameters:
        - A dataframe containing the dataset to be obfuscated.
        - A list of fields containing the data to be obfuscated.
    Returns:
        - A new dataframe with the required obfuscation completed.
    
    """
    new_df = df.copy()
    new_df[pii_fields] = "***"
    return new_df


def convert_bytestream_to_df(formatted_bytes, fields):
    """
    This function will identify the format convention of a bytestream representing a dataset. 
    The function will then convert this bytestream into a pandas dataframe.
    It will work if the format is valid csv, json or parquet.

    Parameters:
        - formatted_bytes
            A bytestream formatted with csv, json, or parquet.
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
    try:
        bytesio = BytesIO(formatted_bytes)
        df = pd.read_parquet(bytesio)
        if set(fields).issubset(set(df.columns.values.tolist())):
            df_dict["format"] = "parquet"
        else:
            raise TypeError("Failed to interpret bytes as parquet")
    except Exception as e:
        formatted_string = formatted_bytes.decode("utf-8")
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
    df_dict["df"] = df
    print("read the df:\n",df)
    return df_dict


def convert_df_to_formatted_bytestream(df, format):
    """
    This function will convert a dataframe to a bytestream of a desired format.

    Parameters:
        - df
            A dataframe of the dataset to convert into a formatted string.
        - format
            The desired format.
            Must be "csv", "json" or "parquet"

    Returns:
        - A formatted bytestream convertion of the dataframe.
    """
    if format == "csv":
        try:
            csv_string = df.to_csv(index=False)
            csv_bytestream = csv_string.encode("utf-8")
            return csv_bytestream
        except Exception as e:
            raise e
    elif format == "json":
        try:
            json_string = json.dumps(df.to_dict())
            json_bytestream = json_string.encode("utf-8")
            return json_bytestream
        except Exception as e:
            raise e
    elif format == "parquet":
        try:
            return df.to_parquet()
        except Exception as e:
            raise e
