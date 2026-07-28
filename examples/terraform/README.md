# Snakemake AWS Batch — Terraform onboarding example

One `terraform apply` that provisions everything a new Snakemake user needs to run workflows on AWS Batch:

- **S3 bucket** — default storage for Snakemake inputs/outputs (versioned, server-side encrypted, public-access blocked)
- **EC2 compute environment** — SPOT capacity, scales to zero when idle
- **Job queue** — single priority queue backed by the compute environment
- **Executor IAM role** — assumed by the process running Snakemake; minimal Batch + S3 + IAM permissions
- **Job IAM role** — assumed by running containers; scoped S3 read/write on the bucket
- **ECS instance role + Batch service role** — managed-policy roles required by EC2/Batch
- **Security group** — outbound-only; jobs pull images and push results, nothing inbound
- **Optional ECR repository** — for custom container images (`create_ecr_repo = true`)

> **Relationship to `terraform/` at the repo root.**  The root `terraform/` module provisions CI test infrastructure — an ECS instance role and Batch service role used by the `ci_true_api` workflow against a pre-existing environment.  This `examples/terraform/` module is a *self-contained* onboarding stack for new users.  Both modules create ECS instance roles and service roles with the same AWS managed policies; they do *not* share Terraform state.

---

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.3
- AWS credentials configured (`aws configure` or environment variables) with permissions to create IAM roles, Batch compute environments, S3 buckets, EC2 security groups, and optionally ECR repositories
- An existing VPC with at least one subnet that has outbound internet access (public subnet with `MapPublicIpOnLaunch = true`, or private subnet behind a NAT gateway)

---

## Quick start

### 1. Configure variables

Create a `terraform.tfvars` file (git-ignored):

```hcl
region     = "us-east-1"
name_prefix = "myteam"
vpc_id     = "vpc-0abc123def456"
subnet_ids = ["subnet-0abc123def456", "subnet-0def456abc123"]

# Optional overrides
instance_types  = ["c5", "m5", "r5"]   # or ["optimal"] for Batch to choose
max_vcpus       = 512
create_ecr_repo = true                 # set true to get an ECR repo
```

### 2. Apply

```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

### 3. Run Snakemake

After `apply`, Terraform prints the exact CLI flags:

```bash
terraform output -raw snakemake_cli_flags
```

Example output (copy-paste into your shell):

```bash
snakemake \
  --executor aws-batch \
  --aws-batch-region us-east-1 \
  --aws-batch-job-queue arn:aws:batch:us-east-1:123456789012:job-queue/myteam-snakemake-queue \
  --aws-batch-job-role arn:aws:iam::123456789012:role/myteam-snakemake-job-role \
  --default-storage-provider s3 \
  --default-storage-prefix s3://myteam-snakemake-a1b2c3d4 \
  --container-image ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:0.2.1 \
  Snakefile
```

> The container image above is the published runtime image (see [Runtime image](#runtime-image)).  Prefer a pinned tag or digest over `:latest` so runs are reproducible.  The GHCR package is **public**, so the Batch EC2 instances can pull it anonymously with no extra setup.  If your compute environment has no egress to `ghcr.io`, mirror the image into your own ECR instead: set `create_ecr_repo = true`, push a copy, and point `--container-image` at the ECR URL.  The `AmazonEC2ContainerServiceforEC2Role` already grants ECR pull permissions on the instances, so no further auth is needed.  (The ECR credential helper authenticates ECR only — not `ghcr.io` — so ECR mirroring is the simplest in-AWS option.)
>
> See the [Runtime image section of the top-level README](../../README.md#runtime-image) for details.

### 4. Clean up

```bash
# Empty the S3 bucket first (force_destroy = false protects against accidental deletion)
aws s3 rm s3://<bucket> --recursive

terraform destroy
```

---

## Variables

| Name | Type | Default | Description |
|---|---|---|---|
| `region` | string | `us-east-1` | AWS region |
| `name_prefix` | string | `snakemake` | Prefix for all resource names |
| `vpc_id` | string | _(required)_ | VPC ID |
| `subnet_ids` | list(string) | _(required)_ | Subnet IDs for the compute environment |
| `instance_types` | list(string) | `["optimal"]` | EC2 instance families/types |
| `max_vcpus` | number | `256` | Hard vCPU cap for the compute environment |
| `compute_resource_type` | string | `SPOT` | Batch capacity type: `SPOT` (cheaper, interruptible) or `EC2` (on-demand); `SPOT` also provisions a Spot Fleet IAM role |
| `container_image` | string | `ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:0.2.1` | Container image every job runs; prefer a pinned tag/digest. Mirror into ECR if the compute environment has no egress to ghcr.io |
| `create_ecr_repo` | bool | `false` | Create a private ECR repository |
| `ecr_repo_name` | string | `snakemake-batch` | ECR repository name (when enabled) |
| `s3_bucket_name` | string | _(auto)_ | S3 bucket name; auto-generated if empty |
| `allowed_executor_principals` | list(string) | _(account root)_ | IAM principal ARNs allowed to assume the executor role; defaults to the current account root |

## Outputs

| Name | Description |
|---|---|
| `snakemake_cli_flags` | Full copy-paste `snakemake` command with all flags |
| `job_queue_arn` | `--aws-batch-job-queue` value |
| `job_role_arn` | `--aws-batch-job-role` value |
| `executor_role_arn` | IAM role for the Snakemake process |
| `s3_bucket` | `--default-storage-prefix` bucket name |
| `region` | `--aws-batch-region` value |
| `ecr_repository_url` | ECR URL (when `create_ecr_repo = true`) |

---

## Optional IAM add-ons

The executor policy includes only the minimum required permissions.  Uncomment the relevant blocks in `main.tf` → `data "aws_iam_policy_document" "executor_core"` to enable optional features:

| Block | Enables |
|---|---|
| `BatchTagging` | `--aws-batch-tags` / `propagate_tags` (requires `batch:TagResource`) |
| `LogsRead` | CloudWatch log surfacing in job error output (requires `logs:GetLogEvents`) |
| `SpotTermination` | High-confidence spot-termination detection (requires `ecs:DescribeContainerInstances` + `ec2:DescribeInstances`) |
| `CostEstimation` | `--aws-batch-estimate-cost` (requires `pricing:GetProducts` + `ec2:DescribeSpotPriceHistory`) |

---

## Customising the compute environment

The compute environment defaults to **SPOT** with `SPOT_CAPACITY_OPTIMIZED` allocation strategy — the lowest-cost option for batch workloads that tolerate interruption.

To switch to on-demand, set `compute_resource_type = "EC2"` in your `terraform.tfvars`. The module already nulls out the Spot-only fields (`bid_percentage`, `allocation_strategy`, `spot_iam_fleet_role`) and skips the Spot Fleet IAM role when the type is `EC2`, so no manual edits to `main.tf` are needed.

To add a GPU queue, duplicate the `aws_batch_compute_environment` and `aws_batch_job_queue` resources, set `instance_type = ["p3.2xlarge"]` (or similar), and point the new queue at the GPU compute environment.

---

## Runtime image

See the [Runtime image section of the top-level README](../../README.md#runtime-image) for information on the published container image and how to use it with `--container-image`.
