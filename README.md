# Snakemake executor plugin: aws-batch

A Snakemake executor plugin for submitting jobs to AWS Batch. Documentation can be found in the [Snakemake plugin catalog](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/aws-batch.html).

---

## Getting started

The quickest path to a working stack is the Terraform example:

```bash
cd examples/terraform
# edit terraform.tfvars with your VPC/subnet IDs
terraform init && terraform apply
terraform output -raw snakemake_cli_flags
```

See [`examples/terraform/README.md`](examples/terraform/README.md) for the full walkthrough.

---

## Runtime image

A pre-built container image with Snakemake, `snakemake-storage-plugin-s3`, and this plugin is published to the GitHub Container Registry on every release and on pushes to `main`:

```text
ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:latest       # latest release
ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:<version>    # pinned release, e.g. 0.2.1
ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:main         # tip of main
```

Use it with `--container-image`:

```bash
snakemake \
  --executor aws-batch \
  --aws-batch-region us-east-1 \
  --aws-batch-job-queue arn:aws:batch:... \
  --aws-batch-job-role arn:aws:iam::... \
  --default-storage-provider s3 \
  --default-storage-prefix s3://my-bucket \
  --container-image ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:latest \
  Snakefile
```

### Pulling the image

The published image is **public**, so Batch instances can pull it anonymously — no `docker login` or credentials on the job role are required.

If you would rather keep image pulls entirely within AWS (for example to avoid an external registry dependency, or if your compute environment has no egress to `ghcr.io`), mirror the image to your own ECR repository and point the job definitions at that registry. Note that the ECS instance role's ECR credential helper authenticates **ECR only** — it does not authenticate `ghcr.io` — so mirroring is the simplest way to reuse the instance role's existing ECR pull permissions.

### Building a custom image

Use the published image as a base and add your own tools:

```dockerfile
FROM ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:0.2.1

RUN apt-get update && apt-get install -y samtools bwa && rm -rf /var/lib/apt/lists/*
```

Or build from the `docker/Dockerfile` in this repo (run from the repository root so the plugin source is in the build context):

```bash
docker build -t my-snakemake -f docker/Dockerfile .
```
