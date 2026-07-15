# AWS Credentials 

This plugin assumes you have setup AWS CLI credentials in ~/.aws/credentials. For more
information see [aws cli configuration](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html).

# AWS Infrastructure Requirements

The snakemake-executor-plugin-aws-batch requires an EC2 compute environment and a job queue
to be configured. The plugin repo [contains terraform](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/tree/main/terraform) used to setup 
the requisite AWS Batch infrastructure. 

Assuming you have [terraform](https://developer.hashicorp.com/terraform/install) 
installed and aws cli credentials configured, you can deploy
the required infrastructure as follows: 

```
cd terraform
terraform init
terraform plan
terraform apply
```

Resource names can be updated by including a terraform.tfvars file that specifies 
variable name overrides of the defaults defined in vars.tf. The outputs variables from  
running terraform apply can be exported as environment variables for snakemake-executor-plugin-aws-batch to use.

SNAKEMAKE_AWS_BATCH_REGION
SNAKEMAKE_AWS_BATCH_JOB_QUEUE
SNAKEMAKE_AWS_BATCH_JOB_ROLE

# Rule-Specific Container Images

By default, all jobs use the global container image specified via `--container-image`. However, you can specify a different container image for individual rules using the `aws_batch_container_image` resource parameter:

```python
rule my_rule:
    input:
        "input.txt"
    output:
        "output.txt"
    resources:
        aws_batch_container_image="my-custom-image:tag"
    shell:
        "process_data.sh {input} {output}"
```

This allows you to use different containers with specialized tools for different rules within the same workflow, rather than requiring all tools to be present in a single container.

Each rule-specific image must still satisfy the same requirements as the global
container image (see [Container Image Requirements](#container-image-requirements)
below): the worker runs `snakemake` inside the container, so the image must have
Snakemake and a compatible storage plugin installed. A plain tool image that does
not include Snakemake will fail to launch the job.

# Example

## Create environment

Install snakemake and the AWS executor and storage plugins into an environment. We 
recommend the use of mamba package manager which can be [installed using miniforge](https://snakemake.readthedocs.io/en/stable/tutorial/setup.html#step-1-installing-miniforge), but these 
dependencies can also be installed using pip or other python package managers. 

```
mamba create -n snakemake-example \
    snakemake snakemake-storage-plugin-s3 snakemake-executor-plugin-aws-batch
mamba activate snakemake-example
```

Clone the snakemake tutorial repo containing the example workflow:

```
git clone https://github.com/snakemake/snakemake-tutorial-data.git
```

Setup and run tutorial workflow on the the executor

```
cd snakemake-tutorial-data 

export SNAKEMAKE_AWS_BATCH_REGION=
export SNAKEMAKE_AWS_BATCH_JOB_QUEUE=
export SNAKEMAKE_AWS_BATCH_JOB_ROLE=

snakemake --jobs 4 \
    --executor aws-batch \
    --aws-batch-region us-west-2 \
    --default-storage-provider s3 \
    --default-storage-prefix s3://snakemake-tutorial-example \
    --verbose
```

# Container Image Requirements

The plugin does **not** auto-deploy the default storage provider to workers
(`auto_deploy_default_storage_provider=False`): workers no longer run
`pip install snakemake-storage-plugin-s3` at startup, because that pulls an
unpinned version whose newer releases require snakemake >= 9 and break
workers running snakemake 8.x. The container image used for jobs must
therefore pre-install a compatible version of the storage plugin (e.g.
`snakemake-storage-plugin-s3`) alongside snakemake itself. Image maintainers
are responsible for pinning a plugin version compatible with the snakemake
version in the image.

# Scheduling Priority (Fair-Share Queues)

AWS Batch fair-share job queues (those with a scheduling policy attached) order
jobs by priority at submit time via `schedulingPriorityOverride`. The plugin
exposes this at two levels, both optional:

- `--aws-batch-scheduling-priority` — a workflow-level default applied to every
  submitted job.
- `aws_batch_scheduling_priority` resource — a per-rule override that takes
  precedence over the workflow-level setting.

Both are ignored by AWS on non-fair-share queues. When neither is set the
`schedulingPriorityOverride` parameter is omitted entirely from the submit call,
keeping submissions byte-identical to the pre-feature behavior.

Workflow-level default (all jobs):

```sh
snakemake --executor aws-batch --aws-batch-scheduling-priority 50 ...
```

Per-rule override (e.g. boost the critical path above background jobs):

```python
rule critical_path:
    resources:
        aws_batch_scheduling_priority=100
    ...
```

# Per-Rule Job Queues

By default all jobs are submitted to the queue given by
`--aws-batch-job-queue`. A rule can override this with the `batch_queue`
resource, e.g. to route jobs to a queue wired to a different compute
environment (ARM vs x86, GPU vs CPU):

```python
rule align:
    resources:
        batch_queue="arn:aws:batch:us-west-2:123456789012:job-queue/arm-queue"
    ...
```

Platform detection and job submission both use the resolved per-rule queue.

# Task Timeout

By default jobs have no timeout. Set `--aws-batch-task-timeout` to impose a
workflow-wide limit (in seconds; minimum 60). A rule can override this with the
`aws_batch_task_timeout` resource, e.g. to give a long-running alignment step
more time while keeping a tight limit on bookkeeping rules:

```python
rule align:
    resources:
        aws_batch_task_timeout=14400  # 4 h
    ...
```

The per-rule resource takes precedence over `--aws-batch-task-timeout`. When
neither is set, AWS Batch imposes no timeout. When set, the value must be at
least 60 seconds (the AWS minimum).

# Shared Memory (`/dev/shm`)

On EC2/ECS containers `/dev/shm` defaults to 64 MB, which is too small for
tools that stage large in-memory indexes (e.g. bwa-mem2 shared-memory
indexes). A rule can enlarge it via the `shared_memory_size_mb` resource:

```python
rule align:
    resources:
        shared_memory_size_mb=4096
    ...
```

This sets `linuxParameters.sharedMemorySize` on the job definition. It only
applies on EC2 queues — Fargate does not honor
`linuxParameters.sharedMemorySize`, so the resource is ignored there.

# Job Tags

Tags from `--aws-batch-tags` are applied to every job definition and job
submitted by the plugin. In addition, dynamic tags can be supplied via the
`SNAKEMAKE_AWS_BATCH_JOB_TAGS` environment variable as comma-separated
`KEY=VALUE` pairs:

```bash
export SNAKEMAKE_AWS_BATCH_JOB_TAGS="run_id=2024-06-01,team=genomics"
```

Environment variable tags are merged with `--aws-batch-tags` and take
precedence on key conflicts. This enables per-run cost tracking: a
coordinator job can set the variable so that all child jobs it submits
inherit the run-specific tags. AWS Batch allows at most 50 tags per job;
malformed pairs (missing `=` or an empty key) raise an error at submission
time.

# Pre-existing Job Definitions

By default the plugin registers a fresh AWS Batch job definition for every job
and deregisters it afterward.  Accounts where job definitions are managed by
infrastructure tooling (Terraform, CloudFormation) can opt out of this with
`--aws-batch-job-definition`.  When set, the plugin skips
`RegisterJobDefinition`/`DeregisterJobDefinition` entirely and instead submits
with the supplied definition, pushing per-job specifics (command, environment
variables, vcpu/mem/gpu) through `containerOverrides`.

```bash
snakemake --executor aws-batch \
    --aws-batch-job-definition my-snakemake-def:3 \
    ...
```

The value can be a bare name (`my-def`), a name:revision pair (`my-def:3`),
or a full ARN
(`arn:aws:batch:us-east-1:123456789012:job-definition/my-def:3`).

A rule can override the setting for a specific job with the
`aws_batch_job_definition` resource (mirrors the `batch_queue` pattern):

```python
rule align:
    resources:
        aws_batch_job_definition="gpu-enabled-def:2"
    ...
```

**Incompatible combinations** — the following raise a `WorkflowError` at
submission time because they are only meaningful when the plugin builds the
definition:

- `--aws-batch-job-role` (`job_role`): the job role is baked into the
  definition at registration time and cannot be overridden via
  `containerOverrides`.
- The per-rule `shared_memory_size_mb` resource: `linuxParameters.sharedMemorySize`
  is a definition-level field.

The `--aws-batch-container-image` (`container_image`) setting and the per-rule
`aws_batch_container_image` resource are both silently ignored in this mode — the
container image is taken from the pre-existing definition.

Task timeout and scheduling priority **are** still honored: `--aws-batch-task-timeout`
(and the per-rule `aws_batch_task_timeout` resource) and the scheduling priority
(`--aws-batch-scheduling-priority` / the per-rule `aws_batch_scheduling_priority`
resource) travel as `SubmitJob`'s top-level `timeout` and `schedulingPriorityOverride`
fields, so they apply to pre-existing definitions just as they do to dynamically
registered ones.

# Preflight Validation

At startup (before submitting any job) the executor verifies that the
configured job queue is `ENABLED` and `VALID`, that its compute environment(s)
are `ENABLED`/`VALID` with `maxvCpus > 0`, and — when `--aws-batch-job-role` is
set and `iam:GetRole` is available — that the job role exists. A confirmed
misconfiguration (a disabled/invalid queue or compute environment, `maxvCpus=0`,
or a non-existent job role) fails fast with a clear error, so you don't wait for
jobs that could never start.

The check is deliberately conservative about *uncertainty*: a transient API
error, a queue mid-update (`status` `CREATING`/`UPDATING`), or a missing
`batch:Describe*` / `iam:GetRole` permission is treated as a warning and the
workflow proceeds. It uses the `batch:DescribeJobQueues` /
`batch:DescribeComputeEnvironments` permissions the executor already needs, plus
the optional `iam:GetRole` for the job-role check.
