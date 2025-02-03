# README

This directory contains terraform templates to deploy the minimum required AWS 
infrastructure for the snakemake_executor_plugin_aws_batch. Update vars.tf to 
use the resource names and attribute values suitable for your environment, then run:

```
terraform init
terraform plan
terraform apply
```