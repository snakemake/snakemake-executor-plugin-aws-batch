provider "aws" {
  region = var.aws_provider_region
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ecs_instance_role" {
  name               = var.ecs_instance_role_name
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = var.ecs_instance_role_policy_arn
}

resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = var.ecs_instance_role_name
  role = aws_iam_role.ecs_instance_role.name
}

data "aws_iam_policy_document" "batch_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["batch.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "aws_batch_service_role" {
  name               = var.aws_batch_service_role_name
  assume_role_policy = data.aws_iam_policy_document.batch_assume_role.json
}

resource "aws_iam_role_policy_attachment" "aws_batch_service_role" {
  role       = aws_iam_role.aws_batch_service_role.name
  policy_arn = var.aws_batch_service_role_policy_arn
}

resource "aws_placement_group" "sample" {
  name     = var.aws_placement_group_name
  strategy = var.aws_placement_group_strategy
}

resource "aws_security_group" "sg01" {
  name = var.aws_security_group_name

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc" "vpc01" {
  cidr_block = var.aws_vpc_cidr_block
}

resource "aws_subnet" "subnet01" {
  vpc_id     = aws_vpc.vpc01.id
  cidr_block = var.aws_subnet_cidr_block
}

resource "aws_batch_compute_environment" "sample" {
  compute_environment_name = var.aws_batch_compute_environment_name

  compute_resources {
    instance_role = aws_iam_instance_profile.ecs_instance_role.arn

    instance_type = var.instance_types

    max_vcpus = var.max_vcpus
    min_vcpus = var.min_vcpus

    placement_group = aws_placement_group.sample.name

    security_group_ids = [aws_security_group.sg01.id]

    subnets = [aws_subnet.subnet01.id]

    type = var.aws_batch_compute_resource_type
  }

  service_role = aws_iam_role.aws_batch_service_role.arn
  type         = var.aws_batch_compute_environment_type
  depends_on   = [aws_iam_role_policy_attachment.aws_batch_service_role]
}


resource "aws_batch_job_queue" "snakequeue" {
  name     = var.aws_batch_job_queue_name
  state    = var.aws_batch_job_queue_state
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.sample.arn
  }
}