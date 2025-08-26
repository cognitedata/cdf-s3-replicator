import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

/***************************************************************************
 * Configuration
 **************************************************************************/
const config = new pulumi.Config();
const vpcId = config.require("vpcId");
const privateRoutableSubnetIds = config.requireObject<string[]>("privateRoutableSubnetIds");

const secretsStack = new pulumi.StackReference(config.require("secrets-stack-name"));
const appAssetsStack = new pulumi.StackReference(config.require("app-assets-stack-name"));

const runtimeSecretArn = secretsStack.requireOutput("runtimeSecretArn");
const observabilitySecretArn = secretsStack.requireOutput("observabilitySecretArn");

const ecrRepoArn = appAssetsStack.requireOutput("ecrRepoArn");
const applicationStartupBucketArn = appAssetsStack.requireOutput("applicationStartupBucketArn");
const applicationStartupBucketNameSsmArn = appAssetsStack.requireOutput("applicationStartupBucketNameSsmArn");
const cdfStorageBucketArn = appAssetsStack.requireOutput("cdfStorageBucketArn");
const cdfStorageBucketNameSsmArn = appAssetsStack.requireOutput("cdfStorageBucketNameSsmArn");
const logGroupName = appAssetsStack.requireOutput("logGroupName");
const logGroupArn = appAssetsStack.requireOutput("logGroupArn");

const image = process.env.CDF_S3_REPLICATOR_IMAGE;
if (!image) {
  throw new Error("Missing environment variable: CDF_S3_REPLICATOR_IMAGE");
}

const awsRegion = aws.config.requireRegion();

/***************************************************************************
 * IAM Role for ECS Task Execution
 **************************************************************************/
const taskExecutionRole = new aws.iam.Role("fargateTaskExecutionRole", {
  assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({
    Service: "ecs-tasks.amazonaws.com",
  }),
});

new aws.iam.RolePolicy("fargateTaskPolicy", {
  role: taskExecutionRole.id,
  policy: {
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Action: ["ecr:GetAuthorizationToken"],
        Resource: "*",
      },
      {
        Effect: "Allow",
        Action: [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:DescribeImages",
          "ecr:BatchCheckLayerAvailability",
        ],
        Resource: ecrRepoArn,
      },
      {
        Effect: "Allow",
        Action: ["secretsmanager:GetSecretValue"],
        Resource: [runtimeSecretArn, observabilitySecretArn],
      },
      {
        Effect: "Allow",
        Action: ["ssm:GetParameter", "ssm:GetParameters"],
        Resource: [applicationStartupBucketNameSsmArn, cdfStorageBucketNameSsmArn],
      },
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
          applicationStartupBucketArn,
          pulumi.interpolate`${applicationStartupBucketArn}/*`,
          cdfStorageBucketArn,
          pulumi.interpolate`${cdfStorageBucketArn}/*`,
        ],
      },
      {
        Effect: "Allow",
        Action: [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:FilterLogEvents"
        ],
        Resource: [pulumi.interpolate`${logGroupArn}:*`],
      }
    ],
  },
});

/***************************************************************************
 * ECS Cluster
 **************************************************************************/
const cluster = new aws.ecs.Cluster("cdfS3ReplicatorCluster");

/***************************************************************************
 * Task Definition
 **************************************************************************/
const taskDefinition = new aws.ecs.TaskDefinition("cdfS3ReplicatorTaskDef", {
  family: "cdf-s3-replicator",
  cpu: "1024",
  memory: "2048",
  networkMode: "awsvpc",
  requiresCompatibilities: ["FARGATE"],
  executionRoleArn: taskExecutionRole.arn,
  containerDefinitions: pulumi.jsonStringify([
    {
      name: "cdf-s3-replicator",
      image,
      essential: true,
      logConfiguration: {
        logDriver: "awslogs",
        options: {
          "awslogs-group": logGroupName,
          "awslogs-region": awsRegion,
          "awslogs-stream-prefix": "cdf",
        },
      },
      secrets: [
        { name: "COGNITE_BASE_URL", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_BASE_URL::` },
        { name: "COGNITE_PROJECT", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_PROJECT::` },
        { name: "COGNITE_TOKEN_URL", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_TOKEN_URL::` },
        { name: "COGNITE_CLIENT_ID", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_CLIENT_ID::` },
        { name: "COGNITE_CLIENT_SECRET", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_CLIENT_SECRET::` },
        { name: "COGNITE_TOKEN_SCOPES", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_TOKEN_SCOPES::` },
        { name: "COGNITE_STATE_DB", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_STATE_DB::` },
        { name: "COGNITE_STATE_TABLE", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_STATE_TABLE::` },
        { name: "COGNITE_EXTRACTION_PIPELINE", valueFrom: pulumi.interpolate`${runtimeSecretArn}:COGNITE_EXTRACTION_PIPELINE::` },
        { name: "AWS_ACCESS_KEY_ID", valueFrom: pulumi.interpolate`${runtimeSecretArn}:AWS_ACCESS_KEY_ID::` },
        { name: "AWS_SECRET_ACCESS_KEY", valueFrom: pulumi.interpolate`${runtimeSecretArn}:AWS_SECRET_ACCESS_KEY::` },
      ],
      environment: [
        { name: "OAUTHLIB_RELAX_TOKEN_SCOPE", value: "1" },
      ]
    },
  ]),
});

/***************************************************************************
 * Networking: Security Group
 **************************************************************************/
const securityGroup = new aws.ec2.SecurityGroup("cdfS3ReplicatorSG", {
  vpcId,
  egress: [{
    fromPort: 0,
    toPort: 0,
    protocol: "-1",
    cidrBlocks: ["0.0.0.0/0"],
  }],
});

/***************************************************************************
 * ECS Fargate Service
 **************************************************************************/
const service = new aws.ecs.Service("cdfS3ReplicatorService", {
  cluster: cluster.arn,
  taskDefinition: taskDefinition.arn,
  desiredCount: 1,
  launchType: "FARGATE",
  networkConfiguration: {
    subnets: privateRoutableSubnetIds,
    securityGroups: [securityGroup.id],
    assignPublicIp: false,
  },
});

export const serviceName = service.name;
export const clusterName = cluster.name;
