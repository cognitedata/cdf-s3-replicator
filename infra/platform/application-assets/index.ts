import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";

const ecrRepository = new aws.ecr.Repository("ecrRepository", {
  name: "cdf-s3-replicator",
});
new aws.ssm.Parameter("ecrRepositorySsmParameter", {
  name: "/cognite/cdf-s3-replicator/cdf-s3-replicatorEcrRepositoryUrl",
  type: "String",
  value: ecrRepository.repositoryUrl,
});

export const ecrRepoUrl = ecrRepository.repositoryUrl;
export const ecrRepoArn = ecrRepository.arn;

const applicationStartupBucket = new aws.s3.BucketV2(
  "applicationStartupBucket",
  {
    forceDestroy: true,
  },
);
const applicationStartupBucketNameSsm = new aws.ssm.Parameter(
  "applicationStartupBucketName",
  {
    name: "/cognite/cdf-s3-replicator/application-startup-bucket-name",
    type: "String",
    value: applicationStartupBucket.bucket,
  },
);

new aws.s3.BucketPublicAccessBlock(
  "applicationStartupBucketPublicAccessBlock",
  {
    bucket: applicationStartupBucket.bucket,
    blockPublicAcls: true,
    blockPublicPolicy: true,
    ignorePublicAcls: true,
    restrictPublicBuckets: true,
  },
);

new aws.s3.BucketServerSideEncryptionConfigurationV2(
  "applicationStartupBucketSse",
  {
    bucket: applicationStartupBucket.bucket,
    rules: [
      {
        applyServerSideEncryptionByDefault: {
          sseAlgorithm: "AES256",
        },
      },
    ],
  },
);

const cdfStorageBucket = new aws.s3.BucketV2(
  "cdfStorageBucket",
  {
    forceDestroy: true,
  },
);
const cdfStorageBucketNameSsm = new aws.ssm.Parameter(
  "cdfStorageBucketName",
  {
    name: "/cognite/cdf-s3-replicator/cdf-storage-bucket-name",
    type: "String",
    value: cdfStorageBucket.bucket,
  },
);

new aws.s3.BucketPublicAccessBlock(
  "cdfStorageBucketPublicAccessBlock",
  {
    bucket: cdfStorageBucket.bucket,
    blockPublicAcls: true,
    blockPublicPolicy: true,
    ignorePublicAcls: true,
    restrictPublicBuckets: true,
  },
);

new aws.s3.BucketServerSideEncryptionConfigurationV2(
  "cdfStorageBucketSse",
  {
    bucket: cdfStorageBucket.bucket,
    rules: [
      {
        applyServerSideEncryptionByDefault: {
          sseAlgorithm: "AES256",
        },
      },
    ],
  },
);

// Manual step: the access credentials will need to be created in the AWS console
const cdfStorageAccessUser = new aws.iam.User("cdfStorageAccessUser", {
  name: "cdf-storage-access-user",
});

new aws.iam.UserPolicy("cdfStorageUserPolicy", {
  user: cdfStorageAccessUser.name,
  policy: pulumi.jsonStringify({
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ],
          Resource: [
            cdfStorageBucket.arn,
            pulumi.interpolate`${cdfStorageBucket.arn}/*`
          ]
        }
      ]
    })
});

/***************************************************************************
 * ECS Log Group
 **************************************************************************/
const logGroup = new aws.cloudwatch.LogGroup("cdfS3ReplicatorLogGroup", {
  name: "/cognite/cdf-s3-replicator",
  retentionInDays: 30,
});

export const applicationStartupBucketName = applicationStartupBucket.bucket;
export const applicationStartupBucketArn = applicationStartupBucket.arn;
export const applicationStartupBucketNameSsmArn =
  applicationStartupBucketNameSsm.arn;
export const cdfStorageBucketName = cdfStorageBucket.bucket;
export const cdfStorageBucketArn = cdfStorageBucket.arn;
export const cdfStorageBucketNameSsmArn = cdfStorageBucketNameSsm.arn;
export const logGroupName = logGroup.name;
export const logGroupArn = logGroup.arn;
