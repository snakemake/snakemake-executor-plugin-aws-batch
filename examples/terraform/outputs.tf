# ---------------------------------------------------------------------------
# Outputs — paste these directly into your Snakemake invocation.
# Run `terraform output` after apply to retrieve them at any time.
# ---------------------------------------------------------------------------

output "snakemake_cli_flags" {
  description = "Copy-paste flags for the snakemake CLI. The default --container-image points at the public GHCR runtime image, which Batch instances can pull anonymously. If your compute environment has no egress to ghcr.io, mirror it into ECR (create_ecr_repo = true) and replace the value with your ECR URL. See examples/terraform/README.md."
  value       = <<-EOT
    snakemake \
      --executor aws-batch \
      --aws-batch-region ${var.region} \
      --aws-batch-job-queue ${aws_batch_job_queue.snakemake.arn} \
      --aws-batch-job-role ${aws_iam_role.job.arn} \
      --default-storage-provider s3 \
      --default-storage-prefix s3://${aws_s3_bucket.snakemake.id} \
      --container-image ${var.container_image} \
      Snakefile
  EOT
}

output "job_queue_arn" {
  description = "ARN of the AWS Batch job queue (--aws-batch-job-queue)."
  value       = aws_batch_job_queue.snakemake.arn
}

output "job_role_arn" {
  description = "ARN of the job IAM role (--aws-batch-job-role)."
  value       = aws_iam_role.job.arn
}

output "executor_role_arn" {
  description = "ARN of the executor IAM role. Assume this role (or attach its policy to your user/role) before running Snakemake."
  value       = aws_iam_role.executor.arn
}

output "s3_bucket" {
  description = "S3 bucket name for Snakemake default storage (--default-storage-prefix s3://<bucket>)."
  value       = aws_s3_bucket.snakemake.id
}

output "region" {
  description = "AWS region (--aws-batch-region)."
  value       = var.region
}

output "ecr_repository_url" {
  description = "ECR repository URL for custom images (only set when create_ecr_repo = true)."
  value       = var.create_ecr_repo ? aws_ecr_repository.snakemake[0].repository_url : null
}
