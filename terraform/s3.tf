resource "aws_s3_bucket" "file_bucket" {
  bucket = "gdpr-obfuscator-data-file-bucket"
  tags = {
    Name        = "Data bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_versioning" "example" {
  bucket = aws_s3_bucket.file_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}