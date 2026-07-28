# Runtime image for snakemake-executor-plugin-aws-batch
#
# Contains Snakemake, the S3 storage plugin, and this executor plugin so that
# job containers have everything they need without an install step at runtime.
#
# Build from the repository root so the plugin source is in the build context:
#   docker build -t my-snakemake -f docker/Dockerfile .
#
# See the Runtime image section of the top-level README for usage.

FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/snakemake/snakemake-executor-plugin-aws-batch"
LABEL org.opencontainers.image.description="Snakemake runtime image with AWS Batch executor and S3 storage plugins"
LABEL org.opencontainers.image.licenses="MIT"

# Install system packages required by Snakemake and common workflow tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Pin Snakemake and the S3 storage plugin so the image is reproducible.
# Update these versions when cutting a new release.
ARG SNAKEMAKE_VERSION=8.30.0
ARG SNAKEMAKE_STORAGE_S3_VERSION=0.3.1

RUN pip install --no-cache-dir \
    "snakemake==${SNAKEMAKE_VERSION}" \
    "snakemake-storage-plugin-s3==${SNAKEMAKE_STORAGE_S3_VERSION}"

# Install the executor plugin from the repository source so the image always
# contains the exact code in this checkout (matching the tag or commit it was
# built from), rather than whatever version happens to be on PyPI.
COPY pyproject.toml README.md ./
COPY snakemake_executor_plugin_aws_batch/ ./snakemake_executor_plugin_aws_batch/
RUN pip install --no-cache-dir .

# Run as a non-root user. Snakemake writes workflow metadata to the working
# directory, so create the user, make /workspace writable, and switch to it.
RUN useradd --create-home --shell /usr/sbin/nologin snakemake \
    && mkdir -p /workspace \
    && chown -R snakemake:snakemake /workspace
WORKDIR /workspace
USER snakemake

ENTRYPOINT ["snakemake"]
