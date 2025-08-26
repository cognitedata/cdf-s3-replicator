#!/usr/bin/sh

set -e
set -o pipefail

CDF_S3_REPLICATOR_SERVICE_NAME="$1"

# Get metadata token for instance metadata API calls
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")

# Get the instance ID for log stream naming
echo "Getting instance ID for log stream naming."
INSTANCE_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)

# Grab non-secrets.
echo "Getting non-secrets."
export CDF_STORAGE_BUCKET_NAME=$(aws ssm get-parameter --name /cognite/cdf-s3-replicator/cdf-storage-bucket-name --query 'Parameter.Value' --output text)

# Grab secrets.
echo "Getting secrets."
APP_SECRET_JSON=$(aws secretsmanager get-secret-value \
  --secret-id "/cognite/cdf-s3-replicator/runtime" \
  --query 'SecretString' \
  --output text)

# Export app secrets
export COGNITE_HOST=$(echo "$APP_SECRET_JSON" | jq -r '.COGNITE_HOST')
export COGNITE_TOKEN_URL=$(echo "$APP_SECRET_JSON" | jq -r '.COGNITE_TOKEN_URL')
export COGNITE_CLIENT_ID=$(echo "$APP_SECRET_JSON" | jq -r '.COGNITE_CLIENT_ID')
export COGNITE_CLIENT_SECRET=$(echo "$APP_SECRET_JSON" | jq -r '.COGNITE_CLIENT_SECRET')

# Run the Docker container.
echo "Running Docker container."
exec docker run --rm --name "$CDF_S3_REPLICATOR_SERVICE_NAME" \
  -e CDF_STORAGE_BUCKET_NAME \
  -e COGNITE_HOST \
  -e COGNITE_TOKEN_URL \
  -e COGNITE_CLIENT_ID \
  -e COGNITE_CLIENT_SECRET \
#   -p 3000:3000 \ # IMPORTANT: Uncomment this line if you want to expose the service on port 3000
  --log-driver awslogs \
  --log-opt awslogs-group="/cognite/cdf-s3-replicator/application" \
  --log-opt awslogs-stream="$INSTANCE_ID" \
  "$CDF_S3_REPLICATOR_IMAGE"
