terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# ---------------------------------------------------------------------------
# S3 bucket — used as Snakemake default storage (snakemake-storage-plugin-s3)
# ---------------------------------------------------------------------------

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  bucket_name = var.s3_bucket_name != "" ? var.s3_bucket_name : "${var.name_prefix}-snakemake-${random_id.bucket_suffix.hex}"

  # Principals allowed to assume the executor role. Default to the current
  # account root (simple single-account onboarding); callers can narrow the
  # scope (or enable cross-account use) by setting allowed_executor_principals.
  executor_principals = length(var.allowed_executor_principals) > 0 ? var.allowed_executor_principals : ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
}

resource "aws_s3_bucket" "snakemake" {
  # checkov:skip=CKV_AWS_145: SSE-S3 (AES256, configured below) is sufficient for
  # an onboarding example; production deployments should switch to a
  # customer-managed KMS key via kms_master_key_id.
  bucket        = local.bucket_name
  force_destroy = false

  tags = {
    Name      = local.bucket_name
    ManagedBy = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "snakemake" {
  bucket = aws_s3_bucket.snakemake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "snakemake" {
  bucket = aws_s3_bucket.snakemake.id

  rule {
    # SSE-S3 (AES256) keeps the example dependency-free. For production, set
    # kms_master_key_id to a customer-managed KMS key.
    #trivy:ignore:AVD-AWS-0132
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "snakemake" {
  bucket                  = aws_s3_bucket.snakemake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Expire old object versions so the versioned bucket does not accumulate cost
# indefinitely, and clean up incomplete multipart uploads. Tune for your needs.
resource "aws_s3_bucket_lifecycle_configuration" "snakemake" {
  bucket = aws_s3_bucket.snakemake.id

  # noncurrent_version_expiration requires versioning to be enabled first.
  depends_on = [aws_s3_bucket_versioning.snakemake]

  rule {
    id     = "expire-noncurrent-and-incomplete-uploads"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ---------------------------------------------------------------------------
# Security group — outbound-only; jobs pull images and push results to S3
# ---------------------------------------------------------------------------

resource "aws_security_group" "batch_jobs" {
  name        = "${var.name_prefix}-batch-jobs"
  description = "Outbound-only SG for Snakemake Batch jobs"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound (image pulls, S3, AWS APIs)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name      = "${var.name_prefix}-batch-jobs"
    ManagedBy = "terraform"
  }
}

# ---------------------------------------------------------------------------
# ECS instance role — attached to the EC2 instances that run Batch jobs
# (same pattern as the CI infra in terraform/ at the repo root)
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_instance" {
  name               = "${var.name_prefix}-ecs-instance-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = { ManagedBy = "terraform" }
}

resource "aws_iam_role_policy_attachment" "ecs_instance_managed" {
  role       = aws_iam_role.ecs_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ecs_instance" {
  name = "${var.name_prefix}-ecs-instance-profile"
  role = aws_iam_role.ecs_instance.name
}

# ---------------------------------------------------------------------------
# Batch service role — allows Batch to manage EC2 capacity on your behalf
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "batch_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["batch.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "batch_service" {
  name               = "${var.name_prefix}-batch-service-role"
  assume_role_policy = data.aws_iam_policy_document.batch_assume_role.json

  tags = { ManagedBy = "terraform" }
}

resource "aws_iam_role_policy_attachment" "batch_service_managed" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

# ---------------------------------------------------------------------------
# Spot Fleet role — required by AWS Batch to launch, tag and terminate Spot
# instances on your behalf. Only created when compute_resource_type = "SPOT".
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "spot_fleet_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["spotfleet.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spot_fleet" {
  count              = var.compute_resource_type == "SPOT" ? 1 : 0
  name               = "${var.name_prefix}-snakemake-spot-fleet-role"
  assume_role_policy = data.aws_iam_policy_document.spot_fleet_assume_role.json

  tags = { ManagedBy = "terraform" }
}

resource "aws_iam_role_policy_attachment" "spot_fleet_managed" {
  count      = var.compute_resource_type == "SPOT" ? 1 : 0
  role       = aws_iam_role.spot_fleet[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

# ---------------------------------------------------------------------------
# Executor role — assumed by the process that RUNS Snakemake (your laptop,
# a CI runner, or an EC2 instance). Grants the minimum permissions the
# snakemake-executor-plugin-aws-batch needs.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "executor_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    # Defaults to the current account root (see local.executor_principals).
    # Set allowed_executor_principals to explicit IAM user/role ARNs to keep
    # the assumption scope tight, or to enable cross-account use.
    principals {
      type        = "AWS"
      identifiers = local.executor_principals
    }
  }
}

resource "aws_iam_role" "executor" {
  name               = "${var.name_prefix}-snakemake-executor"
  assume_role_policy = data.aws_iam_policy_document.executor_assume_role.json

  tags = { ManagedBy = "terraform" }
}

data "aws_iam_policy_document" "executor_core" {
  # checkov:skip=CKV_AWS_356: The Batch Describe*/RegisterJobDefinition actions
  # below do not support resource-level permissions, so this statement must use
  # "*". SubmitJob/TerminateJob could be scoped to the queue and job-definition
  # ARNs for a production tightening; kept broad here for a readable onboarding
  # example.
  # Core: submit, track and clean up Batch jobs
  statement {
    sid    = "BatchCore"
    effect = "Allow"
    actions = [
      "batch:SubmitJob",
      "batch:DescribeJobs",
      "batch:DescribeJobQueues",
      "batch:DescribeComputeEnvironments",
      "batch:RegisterJobDefinition",
      "batch:DeregisterJobDefinition",
      "batch:TerminateJob",
    ]
    resources = ["*"]
  }

  # Core: pass the job role to containers
  statement {
    sid       = "PassJobRole"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.job.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      # The job role is assumed by ECS tasks (see job_assume_role trust policy);
      # AWS Batch also references the role when registering job definitions.
      values = ["batch.amazonaws.com", "ecs-tasks.amazonaws.com"]
    }
  }

  # Optional — used by the job-role preflight check; harmless to keep in core.
  # Without it the check is skipped (a wrong role still fails clearly at submission).
  statement {
    sid       = "IamGetJobRole"
    effect    = "Allow"
    actions   = ["iam:GetRole"]
    resources = [aws_iam_role.job.arn]
  }

  # Core: S3 read/write for the Snakemake default storage bucket
  statement {
    sid    = "S3Storage"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.snakemake.arn,
      "${aws_s3_bucket.snakemake.arn}/*",
    ]
  }

  # Optional (uncomment to enable propagate-tags / --aws-batch-tags):
  # statement {
  #   sid     = "BatchTagging"
  #   effect  = "Allow"
  #   actions = ["batch:TagResource"]
  #   resources = ["*"]
  # }

  # Optional (uncomment to enable CloudWatch log surfacing in job error output):
  # statement {
  #   sid     = "LogsRead"
  #   effect  = "Allow"
  #   actions = ["logs:GetLogEvents"]
  #   resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/batch/job:*"]
  # }

  # Optional (uncomment for high-confidence spot-termination detection):
  # statement {
  #   sid     = "SpotTermination"
  #   effect  = "Allow"
  #   actions = [
  #     "ecs:DescribeContainerInstances",
  #     "ec2:DescribeInstances",
  #   ]
  #   resources = ["*"]
  # }

  # Optional (uncomment for --aws-batch-estimate-cost):
  # statement {
  #   sid     = "CostEstimation"
  #   effect  = "Allow"
  #   actions = [
  #     "pricing:GetProducts",
  #     "ec2:DescribeSpotPriceHistory",
  #   ]
  #   resources = ["*"]
  # }
}

resource "aws_iam_policy" "executor_core" {
  name   = "${var.name_prefix}-snakemake-executor-policy"
  policy = data.aws_iam_policy_document.executor_core.json

  tags = { ManagedBy = "terraform" }
}

resource "aws_iam_role_policy_attachment" "executor_core" {
  role       = aws_iam_role.executor.name
  policy_arn = aws_iam_policy.executor_core.arn
}

# ---------------------------------------------------------------------------
# Job role — assumed BY THE CONTAINER when a Batch job runs. Grants read/write
# access to the S3 bucket so jobs can fetch inputs and push outputs.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "job_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "job" {
  name               = "${var.name_prefix}-snakemake-job-role"
  assume_role_policy = data.aws_iam_policy_document.job_assume_role.json

  tags = { ManagedBy = "terraform" }
}

data "aws_iam_policy_document" "job_s3" {
  statement {
    sid    = "S3ReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.snakemake.arn,
      "${aws_s3_bucket.snakemake.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "job_s3" {
  name   = "${var.name_prefix}-snakemake-job-policy"
  policy = data.aws_iam_policy_document.job_s3.json

  tags = { ManagedBy = "terraform" }
}

resource "aws_iam_role_policy_attachment" "job_s3" {
  role       = aws_iam_role.job.name
  policy_arn = aws_iam_policy.job_s3.arn
}

# ---------------------------------------------------------------------------
# Compute environment — EC2 MANAGED. SPOT by default for cost savings; set
# compute_resource_type = "EC2" for on-demand capacity (no Spot interruptions).
# The Spot-only fields below (bid_percentage, allocation_strategy,
# spot_iam_fleet_role) are omitted automatically when type = "EC2".
# ---------------------------------------------------------------------------

resource "aws_batch_compute_environment" "snakemake" {
  compute_environment_name = "${var.name_prefix}-snakemake-ce"
  type                     = "MANAGED"
  state                    = "ENABLED"
  service_role             = aws_iam_role.batch_service.arn

  compute_resources {
    type          = var.compute_resource_type
    instance_role = aws_iam_instance_profile.ecs_instance.arn
    instance_type = var.instance_types
    max_vcpus     = var.max_vcpus
    min_vcpus     = 0

    # Spot-only settings — null (i.e. unset) for an EC2 on-demand environment.
    bid_percentage      = var.compute_resource_type == "SPOT" ? 100 : null
    allocation_strategy = var.compute_resource_type == "SPOT" ? "SPOT_CAPACITY_OPTIMIZED" : null
    spot_iam_fleet_role = var.compute_resource_type == "SPOT" ? aws_iam_role.spot_fleet[0].arn : null

    security_group_ids = [aws_security_group.batch_jobs.id]
    subnets            = var.subnet_ids

    # Batch instances need a public IP (or NAT gateway) to reach AWS APIs.
    # Use subnets with map_public_ip_on_launch=true, or private subnets
    # behind a NAT gateway. The security group above is outbound-only and
    # does not control public IP assignment — that is a subnet setting.
  }

  # All three managed-policy attachments must exist before Batch validates the
  # roles at CreateComputeEnvironment. The CE references the roles by ARN, which
  # orders the roles but not their policy attachments. spot_fleet_managed is
  # count-gated on SPOT; referencing the whole resource address is safe when the
  # count is zero (Terraform treats the empty set as no dependency).
  depends_on = [
    aws_iam_role_policy_attachment.batch_service_managed,
    aws_iam_role_policy_attachment.ecs_instance_managed,
    aws_iam_role_policy_attachment.spot_fleet_managed,
  ]

  tags = { ManagedBy = "terraform" }
}

# ---------------------------------------------------------------------------
# Job queue — single priority queue backed by the compute environment above
# ---------------------------------------------------------------------------

resource "aws_batch_job_queue" "snakemake" {
  name     = "${var.name_prefix}-snakemake-queue"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.snakemake.arn
  }

  tags = { ManagedBy = "terraform" }
}

# ---------------------------------------------------------------------------
# Optional ECR repository for custom container images
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "snakemake" {
  count = var.create_ecr_repo ? 1 : 0

  name = var.ecr_repo_name
  # Immutable tags keep job runs reproducible: a given tag always resolves to
  # the same image. Mirror images under a new tag (or digest) rather than
  # re-pushing an existing tag.
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name      = var.ecr_repo_name
    ManagedBy = "terraform"
  }
}
