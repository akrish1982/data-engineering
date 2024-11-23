# Configure AWS provider
provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
    Environment = "dev"
    caylentowner = "ananth.tirumanur@caylent.com"
    }  
  }
  
}

# VPC Configuration
resource "aws_vpc" "redshift_vpc" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support = true
  
  tags = {
    Name = "redshift-vpc"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "redshift_igw" {
  vpc_id = aws_vpc.redshift_vpc.id

  tags = {
    Name = "redshift-igw"
  }
}

# Route Table
resource "aws_route_table" "redshift_rt" {
  vpc_id = aws_vpc.redshift_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.redshift_igw.id
  }

  tags = {
    Name = "redshift-rt"
  }
}

# Route Table Association
resource "aws_route_table_association" "redshift_rta" {
  subnet_id = aws_subnet.redshift_subnet.id
  route_table_id = aws_route_table.redshift_rt.id
}

# Subnet
resource "aws_subnet" "redshift_subnet" {
  vpc_id = aws_vpc.redshift_vpc.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "us-east-1a"
  map_public_ip_on_launch = true
  
  tags = {
    Name = "redshift-subnet"
  }
}

# IAM Role
resource "aws_iam_role" "redshift_role" {
  name = "redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy
resource "aws_iam_role_policy" "redshift_policy" {
  name = "redshift-policy"
  role = aws_iam_role.redshift_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "*"
        ]
      }
    ]
  })
}

# Security Group
resource "aws_security_group" "redshift_sg" {
  name = "redshift-sg"
  vpc_id = aws_vpc.redshift_vpc.id

  ingress {
    from_port = 5439
    to_port = 5439
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Redshift Subnet Group
resource "aws_redshift_subnet_group" "subnet_group" {
  name = "redshift-subnet-group"
  subnet_ids = [aws_subnet.redshift_subnet.id]
  
  tags = {
    Name = "redshift-subnet-group"
  }
}

# Redshift Cluster
resource "aws_redshift_cluster" "ecommerce_cluster" {
  cluster_identifier = "ecommerce-cluster"
  database_name = "ecommerce"
  master_username = "admin"
  master_password = "Admin123!" # Change this in production
  
  node_type = "dc2.large"
  cluster_type = "single-node"
  
  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.subnet_group.name
  iam_roles = [aws_iam_role.redshift_role.arn]
  
  skip_final_snapshot = true
  publicly_accessible = true
  
  tags = {
    Environment = "production"
    Project = "ecommerce-analytics"
  }
}