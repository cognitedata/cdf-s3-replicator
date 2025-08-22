#!/bin/bash

set -e
set -o pipefail

echo "Starting app startup script."

yum update -y

# Install Docker and start it.
echo "Installing docker and starting it."
yum -y install docker 
systemctl start docker
systemctl enable docker
usermod -a -G docker ec2-user

# Pull from instance metadata
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INT_DATA_HUB_AWS_ACCOUNT_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r '.accountId')
AWS_REGION=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r '.region')
APPLICATION_NAME=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/application-name)
ENVIRONMENT=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/environment)
COMPLIANCE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/compliance)
DATA_CLASSIFICATION=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/data-classification)

# Change Dynatrace environment and add identifiers
OBSERVABILITY_SECRETS_JSON=$(aws secretsmanager get-secret-value \
  --secret-id "/cognite/cdf-s3-replicator/observability" \
  --query 'SecretString' \
  --output text)
DYNATRACE_CORE_TENANT_TOKEN=$(echo "$OBSERVABILITY_SECRETS_JSON" | jq -r '.DYNATRACE_CORE_TENANT_TOKEN')
/opt/dynatrace/oneagent/agent/tools/oneagentctl \
  --set-tenant=hdx44096 \
  --set-server=https://hdx44096.live.dynatrace.com/communication \
  --set-tenant-token="$DYNATRACE_CORE_TENANT_TOKEN" \
  --set-host-group=CDF-S3-Replicator \
  --set-host-tag=application-name="$APPLICATION_NAME" \
  --set-host-tag=environment="$ENVIRONMENT" \
  --set-host-tag=compliance="$COMPLIANCE" \
  --set-host-tag=data-classification="$DATA_CLASSIFICATION" \
  --restart-service

# Download the run app container script from S3
echo "Copy run app container script from S3..."
CDF_S3_REPLICATOR_STARTUP_BUCKET_NAME=$(aws ssm get-parameter --name /cognite/cdf-s3-replicator/application-startup-bucket-name --query Parameter.Value --output text)
aws s3 cp s3://$CDF_S3_REPLICATOR_STARTUP_BUCKET_NAME/scripts/run-app-container.sh /opt/bin/run-app-container.sh
chmod +x /opt/bin/run-app-container.sh

# Authenticate into ECR
echo "Authenticating with Amazon ECR..."
aws ecr get-login-password | docker login --username AWS --password-stdin $INT_DATA_HUB_AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Substitute the CDF_S3_REPLICATOR_IMAGE variable with the actual image name
CDF_S3_REPLICATOR_IMAGE=<<<CDF_S3_REPLICATOR_IMAGE>>>

# Pull the Docker image
echo "Pulling the Docker image from ECR..."
docker pull $CDF_S3_REPLICATOR_IMAGE

# Create a systemd service file for the Docker container
echo "Copy systemd service file from S3..."
aws s3 cp s3://$CDF_S3_REPLICATOR_STARTUP_BUCKET_NAME/systemd/cdf-s3-replicator.service /etc/systemd/system/cdf-s3-replicator.service
chmod 644 /etc/systemd/system/cdf-s3-replicator.service

# Make sure the cwagent user has the right permissions to access the SSM logs
# https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-common-scenarios.html#CloudWatch-Agent-run-as-user
echo "Setting up permissions for cwagent user..."
usermod -a -G adm cwagent
chown root:adm /var/log/amazon/ssm
chown root:adm /var/log/amazon/ssm/amazon-ssm-agent.log

chmod 750 /var/log/amazon/ssm
chmod 640 /var/log/amazon/ssm/amazon-ssm-agent.log
echo "Permissions set for cwagent user."

# Reload systemd and enable the service
echo "Reloading systemd and enabling service..."
systemctl daemon-reload
systemctl enable cdf-s3-replicator.service
systemctl start cdf-s3-replicator.service
echo "CDF S3 Replicator service started."