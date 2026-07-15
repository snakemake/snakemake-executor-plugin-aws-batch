# Snakemake executor plugin: aws-batch

A Snakemake executor plugin for submitting jobs to AWS Batch. Documentation can be found in the [Snakemake plugin catalog](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/aws-batch.html).

For the minimal IAM policy and the optional tags add-on, see [docs/further.md — Required IAM permissions](docs/further.md#required-iam-permissions).

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
ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:<version>    # a pinned release tag
ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:main         # tip of main
```

Images are published starting from the first release **after** this image-publishing workflow lands. Until that release is cut, use the `:main` tag (rebuilt on every push to `main`); once releases are available, prefer a pinned `:<version>` tag or digest for reproducible runs.

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

> **Maintainer note:** GHCR packages default to **private** on first publish, even when the source repository is public — visibility is not inherited from the repo. After the publish workflow's first successful run, a maintainer must set the package visibility to Public (repo/org → Packages → this package → Package settings → Change visibility). `GITHUB_TOKEN` cannot change package visibility, so this is a one-time manual step; until it is done, anonymous pulls (and therefore Batch jobs) will fail with `denied`/`not found`.

If you would rather keep image pulls entirely within AWS (for example to avoid an external registry dependency, or if your compute environment has no egress to `ghcr.io`), mirror the image to your own ECR repository and point the job definitions at that registry. Note that the ECS instance role's ECR credential helper authenticates **ECR only** — it does not authenticate `ghcr.io` — so mirroring is the simplest way to reuse the instance role's existing ECR pull permissions.

### Building a custom image

Use the published image as a base and add your own tools:

```dockerfile
FROM ghcr.io/snakemake/snakemake-executor-plugin-aws-batch:latest

RUN apt-get update && apt-get install -y samtools bwa && rm -rf /var/lib/apt/lists/*
```

Or build from the `docker/Dockerfile` in this repo (run from the repository root so the plugin source is in the build context):

```bash
docker build -t my-snakemake -f docker/Dockerfile .
```
