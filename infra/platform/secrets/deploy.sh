#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

pulumi config --cwd infra/platform/secrets --stack cognite/$INT_DATA_HUB_ENVIRONMENT

# Update the stack
echo "[START] infra/platform/secrets Pulumi deployment for $INT_DATA_HUB_ENVIRONMENT stack."

pulumi $PULUMI_ACTION --cwd infra/platform/secrets --stack cognite/$INT_DATA_HUB_ENVIRONMENT --yes --skip-preview

echo "[FINISH] infra/platform/secrets Pulumi deployment for $INT_DATA_HUB_ENVIRONMENT stack."
