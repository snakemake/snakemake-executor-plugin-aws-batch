variable "aws_provider_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "us-west-2"
}

variable "ecs_instance_role_name" {
  description = "The name of the ECS instance role"
  type        = string
  default     = "ecs_instance_role01"
}

variable "ecs_instance_role_policy_arn" {
  description = "The ARN of the ECS instance role policy"
  type        = string
  default     = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

variable "aws_batch_service_role_name" {
  description = "The name of the AWS Batch service role"
  type        = string
  default     = "aws_batch_service_role01"
}

variable "aws_batch_service_role_policy_arn" {
  description = "The ARN of the AWS Batch service role policy"
  type        = string
  default     = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}


variable "aws_placement_group_name" {
  description = "The name of the placement group"
  type        = string
  default     = "sample"
}

variable "aws_placement_group_strategy" {
  description = "The strategy of the placement group"
  type        = string
  default     = "cluster"
}

variable "aws_security_group_name" {
  description = "The name of the security group"
  type        = string
  default     = "sg01"
}

variable "aws_vpc_cidr_block" {
  description = "The CIDR block for the VPC"
  type        = string
  default     = "10.1.0.0/16"
}

variable "aws_subnet_cidr_block" {
  description = "The CIDR block for the subnet"
  type        = string
  default     = "10.1.1.0/24"
}

variable "aws_batch_compute_environment_name" {
  description = "The name of the AWS Batch compute environment"
  type        = string
  default     = "snakecomputeenv"
}

variable "instance_types" {
  description = "The allowed instance types for the compute environment"
  type        = list(string)
  default     = ["c4.xlarge"]
  # , "c4.xlarge", "c4.2xlarge", "c4.4xlarge", "c4.8xlarge"]
}

variable "max_vcpus" {
  description = "The maximum number of vCPUs for the compute environment"
  type        = number
  default     = 16
}

variable "min_vcpus" {
  description = "The minimum number of vCPUs for the compute environment"
  type        = number
  default     = 0
}

variable "aws_batch_compute_resource_type" {
  description = "The type of the AWS Batch compute environment"
  type        = string
  default     = "EC2"
}

variable "aws_batch_compute_environment_type" {
  description = "The type of the AWS Batch compute environment"
  type        = string
  default     = "MANAGED"
}

variable "aws_batch_compute_resource_allocation_strategy" {
  description = "The allocation strategy of the AWS Batch compute environment"
  type        = string
  default     = "BEST_FIT"
}

variable "aws_batch_job_queue_name" {
  description = "The name of the AWS Batch job queue"
  type        = string
  default     = "snakejobqueue"
}

variable "aws_batch_job_queue_state" {
  description = "The state of the AWS Batch job queue"
  type        = string
  default     = "ENABLED"
}