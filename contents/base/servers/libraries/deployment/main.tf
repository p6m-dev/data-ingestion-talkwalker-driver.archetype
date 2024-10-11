terraform {

  required_version = ">= 1.2.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
}

provider "aws" {
  region = var.region
}



module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 3.0"

  name                          = "${var.name}-vpc"
  cidr                          = "10.0.0.0/16"
  azs                           = ["us-east-1a", "us-east-1b",]
  public_subnets                = ["10.0.1.0/24", "10.0.2.0/24"]
#  private_subnets               = ["10.0.3.0/24", "10.0.4.0/24"]
#  enable_nat_gateway            = true
#  single_nat_gateway            = true
  enable_dns_hostnames          = true

#   Manage so we can name
#  manage_default_network_acl    = true
#  default_network_acl_tags      = { Name = var.name }
#  manage_default_route_table    = true
#  default_route_table_tags      = { Name = var.name }
#  manage_default_security_group = true
#  default_security_group_tags   = { Name = var.name }
#  public_subnet_tags            = { Name = var.name }
}

resource "aws_security_group" "worker_group_mgmt_one" {
  name_prefix = "worker_group_mgmt_one"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port = 22
    to_port   = 22
    protocol  = "tcp"

    cidr_blocks = [
      "10.0.0.0/8",
    ]
  }
}

#resource "aws_security_group" "eks_msk" {
#  name   = "${var.name}-eks-mask"
#  vpc_id = module.vpc.vpc_id
#}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "19.16.0"

  cluster_name = var.name

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.public_subnets
#  control_plane_subnet_ids = module.vpc.intra_subnets
  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = true

  eks_managed_node_group_defaults = {
    ami_type       = "AL2_x86_64"
    instance_types = ["p3.2xlarge"]

    key_name = var.key-name

    attach_cluster_primary_security_group = true
    vpc_security_group_ids                = [aws_security_group.worker_group_mgmt_one.id]
#      , aws_security_group.eks_msk.id]
  }

  eks_managed_node_groups = {
    p6m = {
      min_size     = 1
      max_size     = 1
      desired_size = 1

      metadata_options = {
        http_endpoint          = "enabled"
        http_tokens            = "required"
        instance_metadata_tags = "enabled"
      }

      labels = {
        Environment = "dev"
      }
    }
  }


  tags = {
    Terraform   = "true"
    Environment = "Ingestion"
  }
}


resource "aws_ecr_repository" "repo" {
  name                 = "${var.name}-repo"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "ecr" {
  repository = aws_ecr_repository.repo.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 2 images"
      action = {
        type = "expire"
      }
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 2
      }
    }]
  })
}

resource "aws_iam_policy" "ecr-policy" {
  name        = "ecr-policy"
  policy      = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Action   = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:GetRepositoryPolicy",
          "ecr:DescribeRepositories",
          "ecr:ListImages",
          "ecr:DescribeImages",
          "ecr:BatchGetImage"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "eks-role" {
  name = "eks_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "eks-erc-attachment" {
  name = "eks-erc-attachment"
  policy_arn = aws_iam_policy.ecr-policy.arn
  roles      = [aws_iam_role.eks-role.name]
}

