terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "gdprobf-backend-bucket"
    key = "terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "GDPR Obfuscator"
      DeployedFrom = "Terraform"
      Repository = "GDPRObfuscatortProject"
      Environment = "dev"
      RetentionDate = "2025-02-01"
    }
  }
}