variable "region" {
  description = "AWS region to deploy resources in."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Short prefix applied to all named resources (e.g. 'myteam'). Keep it alphanumeric-plus-hyphens."
  type        = string
  default     = "snakemake"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.name_prefix))
    error_message = "name_prefix must contain only lowercase letters, digits, and hyphens."
  }
}

variable "vpc_id" {
  description = "VPC in which to place the compute environment. Must have at least one subnet with internet access (public subnet with map_public_ip_on_launch=true, or private subnet with a NAT gateway) so job containers can pull images."
  type        = string

  validation {
    condition     = startswith(var.vpc_id, "vpc-")
    error_message = "vpc_id must look like an AWS VPC ID (vpc-...)."
  }
}

variable "subnet_ids" {
  description = "One or more subnet IDs inside vpc_id. Spot interruptions are less likely when you specify subnets across multiple AZs."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) > 0 && alltrue([for s in var.subnet_ids : startswith(s, "subnet-")])
    error_message = "subnet_ids must contain at least one subnet ID, each starting with subnet-."
  }
}

variable "allowed_executor_principals" {
  description = "IAM principal ARNs (users or roles) allowed to assume the executor role. Defaults to the current account root for simple single-account onboarding; set explicit ARNs to narrow the assumption scope or to enable cross-account use."
  type        = list(string)
  default     = []
}

variable "instance_types" {
  description = "EC2 instance types allowed in the compute environment. 'optimal' lets AWS Batch choose; list explicit types to constrain to a specific family."
  type        = list(string)
  default     = ["optimal"]
}

variable "max_vcpus" {
  description = "Hard cap on vCPUs across the compute environment. Batch will not provision beyond this."
  type        = number
  default     = 256
}

variable "compute_resource_type" {
  description = "Capacity type for the Batch compute environment: 'SPOT' (cheaper, but instances can be interrupted) or 'EC2' (on-demand, no interruptions). 'SPOT' additionally provisions a Spot Fleet IAM role."
  type        = string
  default     = "SPOT"

  validation {
    condition     = contains(["SPOT", "EC2"], var.compute_resource_type)
    error_message = "compute_resource_type must be either \"SPOT\" or \"EC2\"."
  }
}

variable "container_image" {
  description = "Container image for Snakemake jobs. The default points at the project's public runtime image on GHCR, which Batch instances can pull anonymously. If your compute environment has no egress to ghcr.io, mirror the image into ECR (set create_ecr_repo = true, push a copy) and point this at your ECR URL — or supply your own image. Prefer a pinned tag or digest over a mutable tag like ':latest' so runs are reproducible."
  type        = string
  default     = "ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:0.2.1"
}

variable "create_ecr_repo" {
  description = "Set to true to create a private ECR repository for custom container images."
  type        = bool
  default     = false
}

variable "ecr_repo_name" {
  description = "Name of the ECR repository to create (only used when create_ecr_repo = true)."
  type        = string
  default     = "snakemake-batch"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket used as the Snakemake default storage. Must be globally unique. Leave empty to auto-generate a name from name_prefix."
  type        = string
  default     = ""
}
