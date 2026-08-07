# infrastructure/terraform/main.tf
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# 1. VPC for Network Isolation
resource "aws_vpc" "kinetix_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = { Name = "kinetix-vpc" }
}

# 2. Managed Streaming for Kafka (MSK)
resource "aws_msk_cluster" "kinetix_kafka" {
  cluster_name           = "kinetix-msk-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.large"
    client_subnets = [aws_subnet.private_1.id, aws_subnet.private_2.id, aws_subnet.private_3.id]
    security_groups = [aws_security_group.kafka_sg.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
   3. Relational Database Service (RDS) for Postgres
resource "aws_db_instance" "kinetix_postgres" {
  identifier           = "kinetix-db"
  engine               = "postgres"
  engine_version       = "15.4"
  instance_class       = "db.t3.medium"
  allocated_storage    = 20
  db_name              = "kinetix_db"
  username             = "postgres"
  password             = var.db_password # Stored in AWS Secrets Manager in production
  vpc_security_group_ids = [aws_security_group.db_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.kinetix_db_subnet.name
  skip_final_snapshot    = true
}

# 4. Elastic Kubernetes Service (EKS) for BentoML/Flink
# (Simplified for brevity; in production, use the terraform-aws-modules/eks module)
resource "aws_eks_cluster" "kinetix_eks" {
  name     = "kinetix-eks-cluster"
  role_arn = aws_iam_role.eks_cluster_role.arn

  vpc_config {
    subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  }
}