# GDPRObfuscatortProject
This repo is a tool which will obfuscate sensitive data from a file location, to comply with GDPR.

The tool must be supplied with a file location of a file that might contain sensitive data and any fields which contain sensitive data in the file. 

This tool can be run locally or deployed to an aws lambda function.
To run locally:
    - ensure your aws credentials are correctly set up 
    - in CLI run "make run-checks" to confirm everything is working properly
    - use the local_event function from Obfuscation_event.py
To run as a lambda:
    - ensure your aws credentials are correctly set up, and terraform is installed 
    - in CLI run "make run-checks" to confirm everything is working properly
    - navigate to the terraform directory, and in CLI run "terraform init;
    terraform plan;
    terraform apply" to deploy the lambda
    - use the lambda event function from Obfuscation_event.py

The tool should be invoked by sending a JSON string containing the s3 path of the file to obfuscate and a list of fields that must be obfuscated from the file. For example, the input might be:
{
"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
"pii_fields": ["name", "email_address"]
}
This should be passed as an arguement to the chosen event function.
The repo contains 

The target file must be one of:
    a parquet file, 
    csv file encoded with utf-8
    json file encoded with utf-8

The event function will return a bytestream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings.