data "archive_file" "obfuscator_lambda_file" {
  type        = "zip"
  output_file_mode = "0666"
  source_dir = "../main/"   
  output_path = "../terraform/deploy.zip"          
}

data "archive_file" "dependencies" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "../dependency_layer"
  output_path = "../python.zip"
}

resource "aws_lambda_function" "obfuscator_lambda" {
    function_name = "obfuscator_lambda"
    filename = "deploy.zip"
    role = aws_iam_role.lambda_role.arn
    handler = "GDPRObfuscator.lambda_handler"       
    runtime = "python3.11"       
    timeout = 60
    source_code_hash = data.archive_file.obfuscator_lambda_file.output_base64sha256
    layers = [
      aws_lambda_layer_version.dependancies_layer.arn,
      "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:19"
      ]
}

resource "aws_lambda_layer_version" "dependancies_layer" {
  layer_name = "dependancies_layer"
  compatible_runtimes = ["python3.11"]
  compatible_architectures = ["x86_64", "arm64"]
  filename = "../python.zip"
}

# resource "aws_lambda_permission" "lambda_invoke" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.obfuscator_lambda.function_name
#   principal = "s3.amazonaws.com"
#   source_arn = aws_s3_bucket.file_bucket.arn
#   source_account = data.aws_caller_identity.current.account_id
# }


# resource "aws_lambda_permission" "allow_eventbridge" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.obfuscator_lambda.function_name
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.ingestion_scheduler.arn
#   source_account = data.aws_caller_identity.current.account_id
# }

# resource "aws_lambda_function_event_invoke_config" "lambda_invoke_config" {
#   function_name                = aws_lambda_function.ingestion_lambda.function_name
#   maximum_event_age_in_seconds = 60
#   maximum_retry_attempts       = 0
#   qualifier     = "$LATEST"
# }