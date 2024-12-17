resource "aws_iam_role" "lambda_role" {
    name_prefix = "role-lambda"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:PutObject",
               "s3:GetObject",
               "s3:ListBucket",
               "s3:ListAllMyBuckets",
               "s3:DeleteObject"]

    resources = [
      "${aws_s3_bucket.file_bucket.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-obfuscator_lambda"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

# data "aws_iam_policy_document" "cw_document" {
#   statement {

#     actions = [ "logs:CreateLogGroup" ]

#     resources = [
#       "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
#     ]
#   }

#   statement {

#     actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

#     resources = [
#       "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/obfuscator_lambda:*"
#     ]
#   }
# }



# resource "aws_iam_policy" "cw_policy" {
#     name_prefix = "cw-policy-obfuscator_lambda"
#     policy = data.aws_iam_policy_document.cw_document.json
# }

# resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
#     role = aws_iam_role.lambda_role.name
#     policy_arn = aws_iam_policy.cw_policy.arn
# }