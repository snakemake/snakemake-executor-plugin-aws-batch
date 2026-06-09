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
