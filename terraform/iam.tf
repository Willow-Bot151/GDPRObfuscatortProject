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
               "s3:CreateBucket",
               "s3:DeleteObject",
               "s3:DeleteBucket",
               "s3:ListBucket",
               "s3:ListAllMyBuckets",
            ]

    resources = [
      "*",
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
