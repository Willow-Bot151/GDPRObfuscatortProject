# GDPRObfuscatortProject
This python library is a tool which will obfuscate sensitive data from a file location, to comply with GDPR.

The tool must be supplied with a file location of a file that might contain sensitive data and any fields which contain sensitive data in the file. The tool will be invoked by sending a JSON string containing this information. For example, the input might be:
{
"file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
"pii_fields": ["name", "email_address"]
}
The target CSV file might look like this:
student_id,name,course,cohort,graduation_date,email_address
...
1234,'John Smith','Software','2024-03-31','j.smith@email.com'
...

It will create a new file or bytestream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings.
The output will be a byte-stream representation of a file like this:
student_id,name,course,cohort,graduation_date,email_address
...
1234,'***','Software','2024-03-31','***'
...