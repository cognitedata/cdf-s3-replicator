# Data Modeling Replicator Documentation

## Overview

The **Data Modeling Replicator** is a component designed to synchronize and replicate data modeling instances from Cognite Data Fusion (CDF) into S3-based Delta Lake tables. It is part of the `cdf_fabric_replicator` package and is primarily implemented in the `data_modeling.py` file. This tool enables organizations to efficiently export, snapshot, and manage CDF data models for analytics, reporting, and integration with tools like Tableau.

This documentation is intended for both technical and non-technical users. It explains the purpose, main features, and how the replicator works, as well as providing technical details for developers.

---

## Key Concepts

- **CDF (Cognite Data Fusion):** A platform for managing industrial data, including data models, time series, events, and more.
- **Delta Lake:** An open-source storage layer that brings ACID transactions to Apache Spark and big data workloads, often used on S3.
- **S3:** Amazon Simple Storage Service, used here as the storage backend for Delta tables.
- **Data Modeling:** In CDF, this refers to the creation and management of data models, including nodes and edges (relationships).
- **Snapshot:** A point-in-time export of data, optimized for analytics tools like Tableau.

---

## Main Class: `DataModelingReplicator`

### Purpose

The `DataModelingReplicator` class is responsible for:
- Polling CDF for data modeling instances (nodes and edges)
- Writing and updating Delta Lake tables in S3
- Publishing Tableau-friendly snapshots
- Managing state and ensuring efficient, incremental updates

### Initialization

- **Configuration:** Reads from a config file or override path, including S3 destination and data modeling spaces.
- **Metrics:** Integrates with a metrics system for monitoring.
- **State Store:** Tracks progress to support incremental syncs.
- **S3 Setup:** Ensures AWS credentials are available and initializes the S3 client.

### Main Methods

#### `run()`
- The main loop that:
  - Polls configured data modeling spaces
  - Processes and replicates views
  - Publishes snapshots at regular intervals
  - Handles graceful shutdown via a cancellation token

#### `process_spaces()`
- Iterates over configured data modeling spaces and models
- For each view, triggers replication (nodes and edges)

#### `replicate_view()`
- For a given view, processes both node and edge data
- Ensures both types of data are synchronized

#### `_process_instances()`
- Builds and executes queries to fetch node or edge instances from CDF
- Handles paging and incremental updates

#### `_send_to_s3()`
- Writes extracted data to S3 as Delta Lake tables
- Handles both new data and deletions (tombstones)

#### `_publish_space_snapshots()`
- Creates Tableau-optimized snapshots in S3
- Ensures atomic file replacement for consistency

#### `_atomic_replace_files()`
- Ensures that snapshot files are updated atomically, preventing partial updates

#### `_create_empty_edges_table()` and `_create_empty_nodes_table()`
- Generate empty tables with the correct schema for Tableau, ensuring files always exist even if there is no data

---

## How It Works (Simplified)

1. **Configuration:**
   - The replicator reads its configuration, including which CDF spaces and models to replicate, and where in S3 to store the data.

2. **Polling and Replication:**
   - At regular intervals, the replicator queries CDF for new or updated data modeling instances (nodes and edges).
   - Data is written to S3 in Delta Lake format, supporting efficient analytics and updates.

3. **Snapshot Publishing:**
   - Periodically, the replicator creates Tableau-friendly snapshots (Parquet files) in S3.
   - Snapshots are written atomically to avoid partial or inconsistent data.

4. **State Management:**
   - The replicator tracks its progress using a state store, allowing it to resume from where it left off in case of interruption.

5. **Error Handling:**
   - Errors are logged, and the replicator attempts retries for transient issues.
   - Temporary files are cleaned up to avoid clutter in S3.

---

## Usage Notes

- **AWS Credentials:** The replicator requires AWS credentials (access key, secret key, region) to be set in the environment.
- **Configuration:** Ensure the config file specifies the correct CDF spaces, models, and S3 destination.
- **Delta Lake Tables:** Data is stored in S3 as Delta Lake tables, which can be queried by analytics tools or further processed.
- **Snapshots:** Published snapshots are optimized for Tableau but can be used by any tool that supports Parquet files.
- **Scalability:** The replicator is designed to handle large datasets efficiently, with memory optimizations for big tables.

---

## For Developers

- The main entry point is the `DataModelingReplicator` class in `cdf_fabric_replicator/data_modeling.py`.
- The code uses the Cognite Python SDK, Delta Lake Python bindings, and Boto3 for S3 operations.
- Customization can be done by extending the class or modifying the configuration and snapshot logic.
- Logging is provided for troubleshooting and monitoring.

---

## Troubleshooting

- **Missing AWS Credentials:** Ensure `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` are set in your environment.
- **No Data Written:** Check that the config file specifies valid CDF spaces and models, and that the CDF client has access.
- **Partial Snapshots:** The replicator uses atomic file replacement, but if interrupted, temporary files may remain and are cleaned up on the next run.
- **Performance Issues:** For very large tables, the replicator uses memory-efficient processing, but ensure your environment has sufficient resources.

---

## Contact and Support

For further assistance, please refer to the project README or contact the maintainers listed in the repository. 