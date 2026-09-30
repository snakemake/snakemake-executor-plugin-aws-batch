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

# Required IAM permissions

The principal that runs Snakemake (the *executor role*) needs the permissions
below. They are grouped so you can grant only what your submission path uses: a
small always-required core, an add-on for the default (dynamic) job-definition
path, and an add-on for tags.

## Always required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BatchCore",
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob",
        "batch:DescribeJobs",
        "batch:TerminateJob"
      ],
      "Resource": "*"
    }
  ]
}
```

These three actions are exercised on every submission path: the plugin submits
each job, polls it with `DescribeJobs`, and terminates it on cancellation or
shutdown.

## Add-on: default (dynamic) job-definition path

These permissions are used unless you point the plugin at a pre-existing job
definition (`--aws-batch-job-definition` or the per-rule
`aws_batch_job_definition` resource). On the default path the plugin inspects
the queue, registers a job definition per job, and deregisters it afterwards; a
pre-existing definition is submitted against directly with `containerOverrides`
and needs none of these.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BatchDynamicJobDefinition",
      "Effect": "Allow",
      "Action": [
        "batch:RegisterJobDefinition",
        "batch:DeregisterJobDefinition",
        "batch:DescribeJobQueues",
        "batch:DescribeComputeEnvironments"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassJobRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<account-id>:role/<job-role-name>"
    }
  ]
}
```

- `batch:DescribeJobQueues` and `batch:DescribeComputeEnvironments` let the
  plugin detect whether a queue is backed by EC2 or Fargate so it builds a
  compatible job definition.
- `batch:RegisterJobDefinition` and `batch:DeregisterJobDefinition` register the
  per-job definition and deregister it afterwards.
- The `PassJobRole` statement covers passing the job role (`--aws-batch-job-role`
  / `SNAKEMAKE_AWS_BATCH_JOB_ROLE`) into the definition the plugin registers.
  AWS enforces `iam:PassRole` where the role is supplied — at
  `RegisterJobDefinition` — so it is only needed on this path, scoped to the
  job-role ARN. The plugin refuses `--aws-batch-job-role` together with a
  pre-existing definition: there the job role is baked into the definition by
  whoever registered it (who needed `iam:PassRole` at that point), and the
  executor never passes it, so `iam:PassRole` can be omitted. If a `SubmitJob`
  is ever denied citing `iam:PassRole`, grant it scoped to the definition's role
  ARN.

## Add-on: tags (`--aws-batch-tags` or `SNAKEMAKE_AWS_BATCH_JOB_TAGS`)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BatchTagResource",
      "Effect": "Allow",
      "Action": "batch:TagResource",
      "Resource": "*"
    }
  ]
}
```

`batch:TagResource` authorizes the tag-on-create used when jobs and job
definitions are submitted with tags; without it, submission fails with
`AccessDenied` whenever tags are configured. When tags are set the plugin also
enables `propagateTags` so the Batch job's tags reach the underlying ECS task,
where the billed compute runs. AWS Batch creates and tags that ECS task itself,
using its service role rather than the executor's credentials, so the executor
role needs no ECS permissions. The AWS-managed service-linked role
(`AWSServiceRoleForBatch`) already includes `ecs:TagResource`; if your compute
environment uses a custom Batch service role and ECS tag authorization is
enabled in your account, grant `ecs:TagResource` to that service role.

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

The plugin supports Snakemake 8 and 9 and does **not** auto-deploy the default
storage provider to workers (`auto_deploy_default_storage_provider=False`):
workers do not run `pip install snakemake-storage-plugin-s3` at startup, because
an unpinned install can pull a storage-plugin version whose
`snakemake-interface-storage-plugins` requirement does not match the Snakemake
major baked into the image, breaking the worker (e.g. Snakemake 8 needs a plugin
built against `snakemake-interface-storage-plugins` 3.x, while recent Snakemake 9
releases need one built against 4.x). The exact interface major tracks the
installed Snakemake release, so the container image used for jobs must pre-install
a storage-plugin version compatible with its Snakemake version. Image maintainers
are responsible for pinning a compatible pair.

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
least 60 seconds (the AWS minimum). See
[Spot Reclaim Retries](#spot-reclaim-retries) for how it applies to a job that
AWS Batch retries in place.

# Spot Reclaim Retries

When AWS reclaims an EC2 Spot instance, AWS Batch fails the jobs running on it
(their `statusReason` starts with `Host EC2`). By default the plugin treats that like
any other failure, so it spends one of the rule's `--retries` on a new Batch job.
Set `--aws-batch-spot-attempts` (1-10) to have AWS Batch retry a host
termination itself instead, inside the same Batch job:

```sh
snakemake --executor aws-batch --aws-batch-spot-attempts 3 ...
```

Every submitted job then carries a `retryStrategy` with that many attempts that
retries only a host termination and exits on anything else, so a real failure
(bad input, out of memory, a bug) still reaches Snakemake and `--retries` as
before. This keeps the two apart: reclaims no longer use up `--retries`, and
`--retries` need not be raised to absorb them, which would re-run every real
failure that many more times. When the attempts run out, the job fails as it
does today and `--retries` takes over.

A rule can override the setting with the `aws_batch_spot_attempts` resource. For
example, `aws_batch_spot_attempts=1` sends a reclaim straight to Snakemake, so a
rule whose queue depends on the attempt can move to an on-demand queue after one
reclaim:

```python
rule align:
    resources:
        aws_batch_spot_attempts=1,
        batch_queue=lambda wildcards, attempt: (
            SPOT_QUEUE if attempt == 1 else ON_DEMAND_QUEUE
        ),
    ...
```

The per-rule resource takes precedence over `--aws-batch-spot-attempts`. When
neither is set, the plugin sends no `retryStrategy`, so a pre-existing job
definition's own, if any, applies. The strategy is set on `SubmitJob`, so it
also applies to (and overrides the `retryStrategy` of) a pre-existing job
definition. A group job runs as one Batch job and gets the smallest of its
members' values, so a member with `aws_batch_spot_attempts=1` opts the group
out. A member whose resource is a function of inputs that do not exist yet when
the group is submitted counts as not setting it (it uses the setting). An invalid `--aws-batch-spot-attempts` fails at startup. An invalid
resource stops the workflow, cancelling its running jobs, when a job of that
rule is submitted (like an invalid `aws_batch_task_timeout`), so a value
computed from `attempt` must stay within 1-10 on every attempt.

Only an EC2 host termination is retried. A Fargate Spot interruption (possible
with a pre-existing job definition on a Fargate Spot queue) is reported
differently, so it is not retried in the job and reaches `--retries` as before.
A task timeout (`--aws-batch-task-timeout` / `aws_batch_task_timeout`) applies
to each attempt, so a job that is reclaimed and retried can run for up to
`attempts` times the timeout in all.

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

# Task Logs

Unless told otherwise, AWS Batch sends every job's output to the CloudWatch
Logs group `/aws/batch/job`, which all Batch jobs in the account and region
share. Because CloudWatch Logs grants read access per log group, anyone who may
read one workflow's job logs there may read every workflow's. Retention and KMS
encryption are also set per group. To send a workflow's jobs to their own group,
set `--aws-batch-log-group`:

```bash
snakemake --executor aws-batch \
    --aws-batch-log-group /snakemake/my-project \
    ...
```

This sets the `awslogs` log driver on each job definition the plugin registers,
with `awslogs-group` set to the given group and `awslogs-region` to
`--aws-batch-region`. It takes the group's name, not its ARN. The group must
already exist (the plugin does not create it), and the compute environment's ECS
instance role needs `logs:CreateLogStream` and `logs:PutLogEvents` on it. The
executor role needs no additional permissions; with `logs:DescribeLogGroups` it
also checks at startup that the group exists (see [Preflight
Validation](#preflight-validation)).

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
malformed pairs (missing `=` or an empty key) raise an error at startup,
during the preflight check, before any job is submitted.

When tags are present the plugin also sets `propagateTags=True` on
`submit_job` so that the tags reach the underlying ECS task. Without this,
tags are visible on the Batch job object but absent from the ECS task that
incurs the actual EC2/ECS spend, making them invisible in Cost Explorer.
Batch tags the ECS task using its own service role, so if your compute
environment uses a custom Batch service role and ECS tag authorization is
enabled, that service role needs `ecs:TagResource` (see
[Required IAM permissions](#required-iam-permissions)).

# Pre-existing Job Definitions

By default the plugin registers a fresh AWS Batch job definition for every job
and deregisters it afterward.  Accounts where job definitions are managed by
infrastructure tooling (Terraform, CloudFormation) can opt out of this with
`--aws-batch-job-definition`.  When set, the plugin skips
`RegisterJobDefinition`/`DeregisterJobDefinition` entirely and instead submits
with the supplied definition, pushing per-job specifics (command, environment
variables, vcpu/mem/gpu) through `containerOverrides`.

Because the pre-existing definition supplies its own role, `--aws-batch-job-role`
is **not required** in this mode (it *is* required in the default register-per-job
mode). Providing it alongside `--aws-batch-job-definition` is rejected — see
*Incompatible combinations* below.

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

**Incompatible combinations** — the following are rejected with a
`WorkflowError` because they are only meaningful when the plugin builds the
definition:

- `--aws-batch-job-role` (`job_role`): the job role is baked into the
  definition at registration time and cannot be overridden via
  `containerOverrides`. Combined with the global `--aws-batch-job-definition` it
  is rejected at **startup** (preflight); combined with a per-rule
  `aws_batch_job_definition` resource it is rejected when that job is submitted.
- The per-rule `shared_memory_size_mb` resource: `linuxParameters.sharedMemorySize`
  is a definition-level field (rejected at job submission).
- `--aws-batch-log-group` (`log_group`): the log configuration is a
  definition-level field (rejected at job submission).

The `--aws-batch-container-image` (`container_image`) setting and the per-rule
`aws_batch_container_image` resource are both silently ignored in this mode — the
container image is taken from the pre-existing definition.

Task timeout, scheduling priority and spot attempts **are** still honored:
`--aws-batch-task-timeout` (and the per-rule `aws_batch_task_timeout` resource),
the scheduling priority (`--aws-batch-scheduling-priority` / the per-rule
`aws_batch_scheduling_priority` resource) and the spot attempts
(`--aws-batch-spot-attempts` / the per-rule `aws_batch_spot_attempts` resource)
travel as `SubmitJob`'s top-level `timeout`, `schedulingPriorityOverride` and
`retryStrategy` fields, so they apply to pre-existing definitions just as they
do to dynamically registered ones.

# Preflight Validation

At startup (before submitting any job) the executor sanity-checks the AWS Batch
configuration so you don't wait on jobs that could never start. It verifies that
the configured job queue is `ENABLED` and not in a failed/deleting state
(`status` `INVALID`/`DELETING`/`DELETED`), that at least one of its compute
environments is usable (`ENABLED`, not in a failed/deleting state, and
`maxvCpus > 0` — AWS Batch falls back across the queue's
`computeEnvironmentOrder`, so one healthy environment is enough), and — when
`--aws-batch-job-role` is set and `iam:GetRole` is available — that the job role
exists, and — when `--aws-batch-log-group` is set and `logs:DescribeLogGroups`
is available — that the log group exists. A confirmed misconfiguration (a
disabled/failed queue, a queue with no usable compute environment, a
non-existent job role, or a malformed or non-existent log group) fails fast with
a clear error.

The check is deliberately conservative about *uncertainty*: a transient API
error, a queue mid-update (`status` `CREATING`/`UPDATING`), or a missing
`iam:GetRole` or `logs:DescribeLogGroups` permission is logged as a warning and
the check is skipped rather than failing the workflow. It reuses the
`batch:DescribeJobQueues` / `batch:DescribeComputeEnvironments` permissions the
executor already needs for platform detection (so a run missing those is not
blocked by preflight, but will still fail later when the job definition is
built), plus the optional `iam:GetRole` for the job-role check and
`logs:DescribeLogGroups` for the log-group check.

When tags are configured (via `--aws-batch-tags` or the
`SNAKEMAKE_AWS_BATCH_JOB_TAGS` environment variable), the executor additionally
runs a tag/untag round-trip on the job queue as a best-effort *proxy* for the
`batch:TagResource` permission, so a missing permission surfaces as a warning at
startup rather than only as an opaque `AccessDenied` an hour into the run. It
probes the queue, whereas jobs are tagged on the job and job-definition
resources, so an IAM policy that scopes `batch:TagResource` per resource cannot
be fully verified this way — a denial is therefore logged as a warning (it does
**not** block the run) and a pass is a strong hint, not a guarantee. The probe
needs `batch:TagResource` (and `batch:UntagResource` to remove the throwaway
`snakemake-preflight` tag it writes; if that cleanup untag is denied, the tag is
left on the queue). Depending on your account's tag-authorization settings you
may additionally need `ecs:TagResource`.
