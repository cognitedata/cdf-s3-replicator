# Extractor Configuration Guide

This document explains how the extractor configuration works in the `cdf_fabric_replicator` project, with detailed information about configuration management, environment variable resolution, and the `retrieve_pipeline_config_standalone` method.

## What is Extractor Configuration?

Extractor configuration is a comprehensive system for managing and retrieving settings needed for connecting and interacting with Cognite Data Fusion (CDF) extraction pipelines. The system supports:

- **Environment variable resolution**: Automatic substitution of `${VARIABLE_NAME}` patterns in configuration files
- **Pipeline configuration management**: Reading and writing extraction pipeline configurations to/from CDF
- **Multi-source configuration**: Support for both local YAML files and remote CDF configurations

## Why is this Important?

When working with data pipelines, it is crucial to have a consistent and reliable way to access and update configuration settings. This extractor configuration system provides:

- **Dynamic configuration**: Retrieve the current pipeline configuration from CDF
- **Environment variable support**: Securely manage sensitive information using environment variables
- **Configuration validation**: Ensure that the system is always using correct and complete settings
- **Flexible deployment**: Support for different environments (development, staging, production)

## How Does It Work?

### Configuration Sources

The extractor configuration system reads settings from multiple sources:

1. **Environment variables**: Stored in `.env` files or system environment variables
2. **YAML configuration files**: Human-readable structured configuration files
3. **CDF Pipeline Configurations**: Remote configurations stored in Cognite Data Fusion

### Environment Variable Resolution

The system includes a sophisticated `EnvironmentVariableResolver` class that:

- **Detects variable patterns**: Finds `${VARIABLE_NAME}` patterns in configuration strings
- **Checks availability**: Verifies which environment variables actually exist
- **Selective replacement**: Only replaces variables that are found in the environment
- **Recursive resolution**: Handles nested YAML structures with environment variables

## Key Classes and Methods

### CdfExtractorConfig Class

The main extractor class that extends `Extractor[Config]` and provides comprehensive configuration management.

**Key features:**
- Automatic environment variable loading from `.env` files
- Integration with Cognite Data Fusion extraction pipelines
- YAML configuration processing with environment variable resolution
- Configuration validation and error handling

### EnvironmentVariableResolver Class

A utility class for handling environment variable resolution in configuration files.

**Key methods:**
- `extract_variable_names(text)`: Finds all `${VAR_NAME}` patterns in a string
- `check_env_variables_exist(variable_names)`: Verifies which variables exist in the environment
- `resolve_string(text)`: Replaces environment variables in a string
- `resolve_yaml_structure(obj)`: Recursively resolves variables in nested YAML structures

### Key Method: `retrieve_pipeline_config_standalone`

This static method is the core functionality for fetching extraction pipeline configurations from CDF.

#### What does this method do?

The `retrieve_pipeline_config_standalone` method:
1. **Connects to CDF**: Uses the provided configuration object to establish a connection
2. **Retrieves Pipeline Config**: Fetches the extraction pipeline configuration using the external ID
3. **Processes Configuration**: Loads the YAML configuration and converts it to a Config object
4. **Returns Updated Config**: Provides a new configuration object with the latest CDF settings

#### Method Signature

```python
@staticmethod
def retrieve_pipeline_config_standalone(config, name: str, extraction_pipeline_external_id: str):
```

#### Parameters

- `config`: The base configuration object
- `name`: The name of the extractor client
- `extraction_pipeline_external_id`: The unique identifier for the extraction pipeline in CDF

#### How it works

1. **Client Creation**: Creates a Cognite client using the provided configuration
2. **Config Retrieval**: Fetches the extraction pipeline configuration from CDF
3. **YAML Processing**: Loads the configuration YAML string into a structured object
4. **Logging**: Provides detailed logging of the retrieval process
5. **Error Handling**: Catches and logs any errors that occur during the process

#### Example Usage

```python
# Retrieve the latest configuration from CDF
updated_config = CdfExtractorConfig.retrieve_pipeline_config_standalone(
    config=base_config,
    name="data_modeling_extractor", 
    extraction_pipeline_external_id="my_pipeline_id"
)

# The updated_config now contains the latest settings from CDF
print(f"S3 Bucket: {updated_config.destination.s3.bucket}")
print(f"S3 Region: {updated_config.destination.s3.region}")
```

### Environment Variable Resolution Methods

#### `resolve_env_vars_in_yaml(yaml_source)`

Resolves environment variables in YAML configuration files or strings.

**Features:**
- Supports both file paths and YAML strings
- Automatically loads `.env` files
- Handles complex nested YAML structures
- Only replaces variables that exist in the environment

#### `load_yaml_with_env(yaml_source)`

Convenience wrapper for loading YAML files with environment variable resolution.

## Running the Extractor

### Prerequisites

1. **Poetry**: Ensure Poetry is installed for dependency management
2. **Environment Variables**: Set up required environment variables (see below)
3. **Configuration File**: Prepare your YAML configuration file

### Required Environment Variables

The extractor requires several environment variables to be set:

```bash
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region

# Cognite Data Fusion Configuration
COGNITE_PROJECT=your_project_name
COGNITE_BASE_URL=https://your-cluster.cognitedata.com
COGNITE_CLIENT_ID=your_client_id
COGNITE_CLIENT_SECRET=your_client_secret
COGNITE_TOKEN_URL=https://login.microsoftonline.com/your-tenant-id/oauth2/v2.0/token
```

### Execution Command

To run the extractor with a remote configuration:

```bash
poetry run python cdf_fabric_replicator build/config_remote.yaml
```

This command will:
1. Load the configuration from `build/config_remote.yaml`
2. Resolve any environment variables in the configuration
3. Connect to CDF and retrieve the latest pipeline configuration
4. Start the extraction process with the resolved configuration

### Configuration File Structure

Your `config_remote.yaml` should include:

```yaml
# Cognite connection settings
cognite:
  project: ${COGNITE_PROJECT}
  base_url: ${COGNITE_BASE_URL}
  client_id: ${COGNITE_CLIENT_ID}
  client_secret: ${COGNITE_CLIENT_SECRET}
  token_url: ${COGNITE_TOKEN_URL}

# S3 destination settings
destination:
  s3:
    bucket: ${AWS_S3_BUCKET}
    region: ${AWS_REGION}
    prefix: ${AWS_S3_PREFIX}

# Extraction pipeline settings
extractor_pipeline:
  external_id: "your_pipeline_external_id"
  dataset_external_id: "your_dataset_external_id"

# Data modeling configuration
data_modeling:
  - space: "your_space_name"
    data_models:
      - external_id: "your_model_id"
        views: ["view1", "view2"]
```

## Error Handling and Troubleshooting

### Common Issues

1. **Missing Environment Variables**: The system will log which required variables are missing
2. **Invalid Pipeline External ID**: Check that the extraction pipeline exists in CDF
3. **Authentication Issues**: Verify your Cognite credentials and permissions
4. **S3 Access Issues**: Ensure AWS credentials have proper S3 permissions

### Logging

The extractor provides detailed logging at different levels:
- **INFO**: General operation information
- **DEBUG**: Detailed processing information
- **ERROR**: Error conditions and failures
- **WARNING**: Non-fatal issues that should be addressed

## Best Practices

1. **Environment Variables**: Store sensitive information in environment variables, not in configuration files
2. **Configuration Validation**: Always test your configuration in a development environment first
3. **Error Monitoring**: Set up proper logging and monitoring for production deployments
4. **Regular Updates**: Periodically retrieve fresh configurations from CDF to stay current
5. **Security**: Use least-privilege access for AWS and Cognite credentials

## Summary

The extractor configuration system provides a robust, flexible approach to managing data extraction pipeline configurations. Key benefits include:

- **Dynamic configuration management**: Retrieve and update settings from CDF
- **Environment variable support**: Secure handling of sensitive information
- **Multi-environment support**: Easy deployment across different environments
- **Comprehensive error handling**: Detailed logging and error reporting
- **Flexible execution**: Support for both local and remote configurations

The `retrieve_pipeline_config_standalone` method ensures your system always uses the most current configuration from CDF, making data extraction reliable and maintainable. 