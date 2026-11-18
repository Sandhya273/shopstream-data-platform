terraform {
  required_version = ">= 1.6"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.60" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
  backend "s3" {
    bucket         = "shopstream-terraform-state"
    key            = "data-platform/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "shopstream-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "shopstream-data-platform", env = var.env, owner = "data-engineering", managed_by = "terraform" }
  }
}
