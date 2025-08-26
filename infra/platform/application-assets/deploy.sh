#!/usr/bin/env bash

# Exit immediately if a comman exits with a non-zero status
set -e

pulumi config --cwd infra/platform/application-assets --stack cognite/$INT_DATA_HUB_ENVIRONMENT

#Upate the stack
echo "[START] infra/platform/application-assets Pulumi deployment for $INT_DATA_HUB_ENVIRONMENT stack."

pulumi $PULUMI_ACTION --cwd infra/platform/application-assets --stack cognite/$INT_DATA_HUB_ENVIRONMENT --yes --skip-preview

echo "[END] infra/platform/application-assets Pulumi deployment for $INT_DATA_HUB_ENVIRONMENT stack."