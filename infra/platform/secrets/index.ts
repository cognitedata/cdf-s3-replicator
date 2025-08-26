import * as aws from "@pulumi/aws";

const runtimeSecret = new aws.secretsmanager.Secret("runtimeSecret", {
   name: "/cognite/cdf-s3-replicator/runtime",
   description: "Secrets for CDF S3 Replicator runtime",
});
export const runtimeSecretName = runtimeSecret.name;
export const runtimeSecretArn = runtimeSecret.arn;
