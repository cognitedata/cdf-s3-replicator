import json
import logging
import os
import time
import boto3
from pathlib import Path
import uuid
from typing import Any, Dict, List, Optional, Union
import pyarrow as pa
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import (
    EdgeResultSetExpression,
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import Equals, HasData
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import CancellationToken, Extractor
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
import pyarrow.parquet as pq

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, DataModelingConfig
from cdf_fabric_replicator.metrics import Metrics

class DataModelingReplicator(Extractor):
    """Streams CDF Data-Modeling instances into S3-based Delta tables."""

    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        override_config_path: Optional[str] = None,
    ):
        """
        Initializes the replicator with configuration, metrics, and cancellation control.
        Sets up the extractor and prepares the local Delta Lake directory.
        """
        super().__init__(
            name="cdf_fabric_replicator_data_modeling",
            description="CDF → Delta-Lake (S3)",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
            config_file_path=override_config_path,
        )
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

        self.s3_cfg = None
        self.base_dir: Path = Path.cwd() / "deltalake"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
        self._s3 = None


    def _ensure_s3(self):
        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION"),
            )


    def run(self) -> None:
        """
        Main run loop for the replicator.
        Polls configured data modeling spaces and publishes snapshots at intervals until cancelled.
        """
        self.s3_cfg = (
            self.config.destination.s3 if self.config.destination else None
        )
        if not self.s3_cfg:
            raise RuntimeError("destination.s3 must be configured")
        self.state_store.initialize()
        if not self.config.data_modeling:
            self.logger.info("No data-modeling spaces configured — exiting.")
            return

        last_snapshot_time = 0
        snapshot_interval = getattr(self.config.extractor, 'snapshot_interval', self.config.extractor.poll_time)

        while not self.stop_event.is_set():
            t0 = time.time()

            self.process_spaces()

            if t0 - last_snapshot_time >= snapshot_interval:
                for dm_cfg in self.config.data_modeling:
                    try:
                        self._publish_space_snapshots(dm_cfg)
                    except Exception as err:
                        self.logger.exception("Snapshot publish failed for %s: %s", dm_cfg.space, err)
                last_snapshot_time = t0
            else:
                next_snapshot_in = int(snapshot_interval - (t0 - last_snapshot_time))
                self.logger.debug("⏭Skipping snapshot publish (next in %ds)", next_snapshot_in)

            delay = max(self.config.extractor.poll_time - (time.time() - t0), 0)
            if delay:
                self.stop_event.wait(delay)


    def _selected_views(self, dm_cfg: DataModelingConfig) -> set[str] | None:
        """
        • None    → replicate every view in the space  (default)
        • set()   → replicate nothing (user passed an explicit empty list)
        • set{…}  → replicate exactly those externalIds
        """
        if dm_cfg.views is not None:
            return set(dm_cfg.views)

        if dm_cfg.data_models:
            wanted: set[str] = set()
            for m in dm_cfg.data_models:
                if m.views is None:
                    mdl = self.cognite_client.data_modeling.data_models.retrieve(
                        space=dm_cfg.space, external_id=m.external_id
                    )
                    wanted.update(v.view_id.external_id for v in mdl.views)
                else:
                    wanted.update(m.views)
            return wanted

        return None

    def process_spaces(self) -> None:
        """Iterate spaces and enqueue just the views we want."""
        for dm_cfg in self.config.data_modeling:
            wanted = self._selected_views(dm_cfg)

            try:
                views = self.cognite_client.data_modeling.views.list(
                    space=dm_cfg.space, limit=-1
                )
            except CogniteAPIError as err:
                self.logger.error("View-list failed for %s: %s", dm_cfg.space, err)
                continue

            if wanted is not None:
                views = [v for v in views if v.external_id in wanted]
                if not views and wanted:
                    missing = wanted.difference({v.external_id for v in views})
                    self.logger.warning(
                        "Requested view(s) %s not found in space '%s'",
                        ", ".join(sorted(missing)), dm_cfg.space
                    )

            for v in views:
                self.replicate_view(dm_cfg, v.dump())

    def replicate_view(self, dm_cfg: DataModelingConfig, view: dict[str, Any]) -> None:
        """Two separate syncs to keep each query small."""
        if view.get("usedFor") != "edge":
            self._process_instances(
                dm_cfg,
                f"{view['space']}_{view['externalId']}_nodes",
                view=view,
                kind="nodes",
            )

        self._process_instances(
            dm_cfg,
            f"{view['space']}_{view['externalId']}_edges",
            view=view,
            kind="edges",
        )


    def _process_instances(self, dm_cfg, state_id, view, kind="nodes"):
        query = (
            self._edge_query_for_view(view) if kind == "edges"
            else self._node_query_for_view(view)
        )
        self._iterate_and_write(dm_cfg, state_id, query)

    def _node_query_for_view(self, view: dict[str, Any]) -> Query:
        vid = ViewId(view["space"], view["externalId"], view["version"])
        props = list(view["properties"])

        return Query(
            with_={
                "nodes": NodeResultSetExpression(
                    filter=HasData(views=[vid]), limit=2000
                )
            },
            select={
                "nodes": Select([SourceSelector(vid, props)])
            },
        )

    def _edge_query_for_view(self, view: dict[str, Any]) -> Query:
        vid, props = (
            ViewId(view["space"], view["externalId"], view["version"]),
            list(view["properties"]),
        )

        if view.get("usedFor") != "edge":
            anchor = NodeResultSetExpression(filter=HasData(views=[vid]), limit=2000)
            return Query(
                with_={
                    "nodes": anchor,
                    "edges_out": EdgeResultSetExpression(from_="nodes",
                                                         direction="outwards",
                                                         limit=2000),
                    "edges_in": EdgeResultSetExpression(from_="nodes",
                                                        direction="inwards",
                                                        limit=2000),
                },
                select={
                    "edges_out": Select(),
                    "edges_in": Select(),
                },
            )

        return Query(
            with_={
                "edges": EdgeResultSetExpression(
                    filter=Equals(
                        ["edge", "type"],
                        {"space": vid.space, "externalId": vid.external_id},
                    ),
                    limit=2000,
                )
            },
            select={"edges": Select([SourceSelector(vid, props)])},
        )


    def _page_contains_future_change(self, result: QueryResult, t0_ms: int) -> bool:
        check = lambda inst: inst.last_updated_time >= t0_ms
        return any(
            check(i) for rs in ("nodes", "edges", "edges_out", "edges_in")
            for i in result.data.get(rs, [])
        )


    def _iterate_and_write(self, dm_cfg: DataModelingConfig, state_id: str, query: Query) -> None:
        """
        Executes a paginated query and writes results to local Delta tables and S3.
        Maintains cursor state and handles failures gracefully with retry logic.
        """
        cursors = self.state_store.get_state(external_id=state_id)[1]
        if cursors:
            query.cursors = json.loads(str(cursors))

        try:
            res = self.cognite_client.data_modeling.instances.sync(query=query)
        except CogniteAPIError:
            query.cursors = None
            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError as e:
                self.logger.error(f"Failed to sync instances. Error: {e}")
                raise e

        query_start_ms = int(time.time() * 1000)
        self._send_to_s3(data_model_config=dm_cfg, result=res)

        while any(len(res.data.get(k, [])) > 0 for k in ("nodes", "edges", "edges_out", "edges_in")):
            if self._page_contains_future_change(res, query_start_ms):
                self.logger.info(
                    "Short-circuiting %s – instance updated after paging started; will continue next poll.",
                    state_id,
                )
                break

            query.cursors = res.cursors
            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError:
                query.cursors = None
                try:
                    res = self.cognite_client.data_modeling.instances.sync(query=query)
                except CogniteAPIError as e:
                    self.logger.error(f"Failed to sync instances. Error: {e}")
                    raise e

            self._send_to_s3(data_model_config=dm_cfg, result=res)

        self.state_store.set_state(external_id=state_id, high=json.dumps(query.cursors))
        self.state_store.synchronize()


    def _raw_prefix(self, dm_space: str) -> str:
        """
        Returns the S3 URI prefix for raw data in the given data modeling space.
        Example: s3://bucket/prefix/raw/<space>
        """
        prefix = self.s3_cfg.prefix.rstrip('/')
        prefix = (prefix + '/') if prefix else ''
        return f"s3://{self.s3_cfg.bucket}/{prefix}raw/{dm_space}"

    def _publish_prefix(self, dm_space: str) -> str:
        """
        Returns the S3 URI prefix for published snapshot data in the given space.
        Example: s3://bucket/prefix/publish/<space>
        """
        prefix = self.s3_cfg.prefix.rstrip('/')
        prefix = (prefix + '/') if prefix else ''
        return f"s3://{self.s3_cfg.bucket}/{prefix}publish/{dm_space}"

    def _current_views_map(self, dm_space: str, selected: set[str] | None = None,
    ) -> Dict[str, Dict[str, Union[int, bool]]]:
        """
        Returns {externalId: {"version": int, "is_edge": bool}}
        The Views API already guarantees each externalId appears once,
        representing its latest version.
        """
        views = self.cognite_client.data_modeling.views.list(space=dm_space, limit=-1)
        mapping = {
            v.external_id: {"version": v.version, "is_edge": v.used_for == "edge"}
            for v in views
            if selected is None or v.external_id in selected
        }
        mapping["_edges"] = {"version": None, "is_edge": True}
        return mapping


    def _publish_space_snapshots(self, dm_cfg: DataModelingConfig) -> None:
        """
        Publishes the latest snapshot for each view and edge in a DM space.
        Loads Delta tables and writes Parquet snapshots to S3.
        Post-publish verification for monitoring.
        """
        dm_space = dm_cfg.space
        wanted = self._selected_views(dm_cfg)
        view_map = self._current_views_map(dm_space, wanted)

        success_count = 0
        for xid, meta in view_map.items():
            is_edge_only = meta["is_edge"]
            view_name = xid if xid != "_edges" else "edges_only"

            try:
                self._write_view_snapshot(dm_space, view_name, is_edge_only)
                success_count += 1
            except Exception as e:
                self.logger.error("Failed to publish %s: %s", view_name, e)

        self.logger.info("Snapshot publish complete: %d/%d views successful", success_count, len(view_map))


    def _delete_s3_prefix(self, uri: str) -> None:
        """Delete *all* S3 objects under the given s3:// URI prefix."""
        self._ensure_s3()
        bucket, prefix = uri[5:].split("/", 1)
        if prefix.startswith("/"):
            prefix = prefix[1:]
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objs = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objs:
                self._s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})


    def _write_snapshot(
            self,
            dm_space: str,
            ext_id: str,
            version: Optional[int],
            is_edge: bool,
    ) -> None:
        """
        Writes a snapshot Parquet file for a given view or edge set.
        Reads the Delta table, deduplicates nodes, and writes to S3.
        Return 's3://bucket/prefix/raw/<space>' (no '/views').
        """
        raw_uri = (
            f"{self._raw_prefix(dm_space)}/views/_edges"
            if is_edge
            else f"{self._raw_prefix(dm_space)}/views/{ext_id}"
        )

        pub_uri = (
            f"{self._publish_prefix(dm_space)}/edges/"
            if is_edge
            else f"{self._publish_prefix(dm_space)}/{ext_id}/"
        )

        try:
            dt = DeltaTable(raw_uri)
            tbl = dt.to_pyarrow_table()
        except (FileNotFoundError, DeltaError) as exc:
            self.logger.warning("No data yet for %s — skipping. (%s)", raw_uri, exc)
            return

        if not is_edge and tbl.num_rows:
            df_nodes = (
                tbl.to_pandas()
                .sort_values("lastUpdatedTime", ascending=False)
                .drop_duplicates(
                    subset=["space", "externalId", "instanceType"], keep="first"
                )
            )

            tbl = pa.Table.from_pandas(df_nodes, preserve_index=False)

        self._delete_s3_prefix(pub_uri)

        pq.write_table(tbl, f"{pub_uri}part-00000.parquet", compression="snappy")


    def _write_view_snapshot(self, dm_space: str, view_xid: str, is_edge_only: bool) -> None:
        """
        Tableau-optimized stable filenames with atomic replacement.
        Always writes to: nodes.parquet and edges.parquet (never deletes directory).
        Uses temporary files to ensure Tableau never sees partial/missing data.
        """
        pub_dir = f"{self._publish_prefix(dm_space)}/{view_xid}/"
        temp_suffix = f"_temp_{uuid.uuid4().hex[:8]}"
        files_to_update = []

        try:
            edge_raw = (
                f"{self._raw_prefix(dm_space)}/views/_edges"
                if is_edge_only else
                f"{self._raw_prefix(dm_space)}/views/{view_xid}/edges"
            )

            try:
                edge_tbl = DeltaTable(edge_raw).to_pyarrow_table()

                temp_edge_path = f"{pub_dir}edges{temp_suffix}.parquet"
                final_edge_path = f"{pub_dir}edges.parquet"

                pq.write_table(edge_tbl, temp_edge_path, compression="snappy")
                files_to_update.append((temp_edge_path, final_edge_path, "edges"))

            except (FileNotFoundError, DeltaError):
                self.logger.info("No edge data for %s, creating empty edges.parquet", view_xid)
                empty_edge_tbl = self._create_empty_edges_table()

                temp_edge_path = f"{pub_dir}edges{temp_suffix}.parquet"
                final_edge_path = f"{pub_dir}edges.parquet"

                pq.write_table(empty_edge_tbl, temp_edge_path, compression="snappy")
                files_to_update.append((temp_edge_path, final_edge_path, "edges"))

            if not is_edge_only:
                node_raw = f"{self._raw_prefix(dm_space)}/views/{view_xid}/nodes"
                try:
                    node_tbl = DeltaTable(node_raw).to_pyarrow_table()

                    if node_tbl.num_rows > 0:
                        df = (
                            node_tbl.to_pandas()
                            .sort_values("lastUpdatedTime", ascending=False)
                            .drop_duplicates(["space", "externalId"], keep="first")
                        )
                        node_tbl = pa.Table.from_pandas(df, preserve_index=False)

                    temp_node_path = f"{pub_dir}nodes{temp_suffix}.parquet"
                    final_node_path = f"{pub_dir}nodes.parquet"

                    pq.write_table(node_tbl, temp_node_path, compression="snappy")
                    files_to_update.append((temp_node_path, final_node_path, "nodes"))
                except (FileNotFoundError, DeltaError):
                    self.logger.info("No node data for %s, creating empty nodes.parquet", view_xid)
                    empty_node_tbl = self._create_empty_nodes_table()

                    temp_node_path = f"{pub_dir}nodes{temp_suffix}.parquet"
                    final_node_path = f"{pub_dir}nodes.parquet"

                    pq.write_table(empty_node_tbl, temp_node_path, compression="snappy")
                    files_to_update.append((temp_node_path, final_node_path, "nodes"))

            if files_to_update:
                self._atomic_replace_files(files_to_update)
            else:
                self.logger.warning("No data files created for %s", view_xid)

        except Exception as e:
            self.logger.error("Snapshot creation failed for %s: %s", view_xid, e)
            self._cleanup_temp_files(pub_dir, temp_suffix)
            raise


    def _atomic_replace_files(self, file_updates: list[tuple[str, str, str]]) -> None:
        """
        Atomically replace multiple files by copying temp files to final locations.

        Args:
            file_updates: List of (temp_path, final_path, description) tuples
        """
        self._ensure_s3()

        try:
            for temp_path, final_path, desc in file_updates:
                temp_bucket, temp_key = temp_path[5:].split("/", 1)
                final_bucket, final_key = final_path[5:].split("/", 1)

                self._s3.copy_object(
                    CopySource={'Bucket': temp_bucket, 'Key': temp_key},
                    Bucket=final_bucket,
                    Key=final_key
                )

        except Exception as e:
            self.logger.error("Failed during atomic file replacement: %s", e)
            raise
        finally:
            for temp_path, _, _ in file_updates:
                try:
                    temp_bucket, temp_key = temp_path[5:].split("/", 1)
                    self._s3.delete_object(Bucket=temp_bucket, Key=temp_key)
                except Exception:
                    pass


    def _cleanup_temp_files(self, pub_dir: str, temp_suffix: str) -> None:
        """Clean up any temporary files that might have been created."""
        try:
            bucket, prefix = pub_dir[5:].split("/", 1)
            if prefix.startswith("/"):
                prefix = prefix[1:]

            self._ensure_s3()
            response = self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            temp_objects = [
                obj for obj in response.get('Contents', [])
                if temp_suffix in obj['Key']
            ]

            if temp_objects:
                delete_objects = [{'Key': obj['Key']} for obj in temp_objects]
                self._s3.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': delete_objects}
                )
        except Exception as e:
            self.logger.warning("Failed to cleanup temp files: %s", e)

    def _create_empty_edges_table(self) -> pa.Table:
        """
        Create an empty edges table with the correct schema for Tableau consistency.
        This ensures Tableau always has a file to connect to, even with 0 rows.
        """
        schema = pa.schema([
            ("space", pa.string()),
            ("instanceType", pa.string()),
            ("externalId", pa.string()),
            ("version", pa.int64()),
            ("startNode.space", pa.string()),
            ("startNode.externalId", pa.string()),
            ("endNode.space", pa.string()),
            ("endNode.externalId", pa.string()),
            ("lastUpdatedTime", pa.int64()),
            ("createdTime", pa.int64()),
            ("deletedTime", pa.int64()),
            ("direction", pa.string()),
        ])
        return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)

    def _create_empty_nodes_table(self) -> pa.Table:
        """
        Create an empty nodes table with the correct schema for Tableau consistency.
        This ensures Tableau always has a file to connect to, even with 0 rows.
        """
        schema = pa.schema([
            ("space", pa.string()),
            ("instanceType", pa.string()),
            ("externalId", pa.string()),
            ("version", pa.int64()),
            ("lastUpdatedTime", pa.int64()),
            ("createdTime", pa.int64()),
            ("deletedTime", pa.int64()),
        ])
        return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


    def _verify_tableau_files(self, dm_space: str, view_xid: str, is_edge_only: bool) -> dict:
        """
        Optional: Verify that Tableau files exist and return metadata.
        Useful for monitoring and debugging.
        """
        pub_dir = f"{self._publish_prefix(dm_space)}/{view_xid}/"
        file_info = {}

        self._ensure_s3()
        bucket, prefix = pub_dir[5:].split("/", 1)
        if prefix.startswith("/"):
            prefix = prefix[1:]

        try:
            response = self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.parquet'):
                    filename = key.split('/')[-1]
                    file_info[filename] = {
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'key': key
                    }

            expected_files = ['edges.parquet']
            if not is_edge_only:
                expected_files.append('nodes.parquet')

            missing_files = [f for f in expected_files if f not in file_info]
            if missing_files:
                self.logger.warning("Missing expected files for %s: %s", view_xid, missing_files)
            else:
                self.logger.debug("All expected Tableau files present for %s", view_xid)

        except Exception as e:
            self.logger.error("Failed to verify Tableau files for %s: %s", view_xid, e)

        return file_info


    def _send_to_s3(self, data_model_config: DataModelingConfig, result: QueryResult) -> None:
        """
        Extracts instance data and appends it to the corresponding S3 Delta table.
        Used during sync to continuously update raw Delta tables.
        """
        for tbl_name, rows in self._extract_instances(result).items():
            self._delta_append(tbl_name, rows, data_model_config.space)


    def _send_to_local(self, data_model_config: DataModelingConfig, result: QueryResult) -> None:
        """
        Extracts instance data and appends it to a local Delta table.
        Useful for local development or testing instead of writing to S3.
        """
        for tbl_name, rows in self._extract_instances(result).items():
            self._delta_append(tbl_name, rows, data_model_config.space)


    @staticmethod
    def _extract_instances(res: QueryResult) -> dict[str, list[dict]]:
        """
        •  Edge-only views  → table "_edges"
        •  Node views       → tables "<view>/nodes" and "<view>/edges"
        """
        out: dict[str, list[dict]] = {}

        for rs_name in ("edges", "edges_out", "edges_in"):
            for e in res.data.get(rs_name, []):
                edge_view_xid = e.type.external_id
                is_edge_only = (rs_name == "edges")
                tbl_name = "_edges" if is_edge_only else f"{edge_view_xid}/edges"
                direction = (
                    None if rs_name == "edges"
                    else ("out" if rs_name.endswith("_out") else "in")
                )

                row = {
                    "space": e.space,
                    "instanceType": "edge",
                    "externalId": e.external_id,
                    "version": e.version,
                    "startNode.space": getattr(e.start_node, "space", None),
                    "startNode.externalId": getattr(e.start_node, "external_id", None),
                    "endNode.space": getattr(e.end_node, "space", None),
                    "endNode.externalId": getattr(e.end_node, "external_id", None),
                    "lastUpdatedTime": e.last_updated_time,
                    "createdTime": e.created_time,
                    "deletedTime": e.deleted_time,
                    **{k: v for p in e.properties.data.values() for k, v in p.items()},
                }
                if direction is not None:
                    row["direction"] = direction

                out.setdefault(tbl_name, []).append(row)

        for n in res.data.get("nodes", []):
            for view_id, props in n.properties.data.items():
                tbl_name = f"{view_id.external_id}/nodes"

                out.setdefault(tbl_name, []).append(
                    {
                        "space": n.space,
                        "instanceType": "node",
                        "externalId": n.external_id,
                        "version": n.version,
                        "lastUpdatedTime": n.last_updated_time,
                        "createdTime": n.created_time,
                        "deletedTime": n.deleted_time,
                        **props,
                    }
                )

        return out


    def _delta_append(self, table: str, rows: List[Dict[str, Any]], space: str) -> None:
        """
           Append rows into the *RAW* Delta table on S-3:
           s3://{bucket}/{prefix}/raw/<space>/views/<table>/…
           """
        if rows:
            null_cols = [k for k in rows[0] if all(r.get(k) is None for r in rows)]
            if null_cols:
                for r in rows:
                    for k in null_cols:
                        r.pop(k, None)

        prefix = self.s3_cfg.prefix.rstrip('/') if self.s3_cfg.prefix else ''
        prefix = (prefix + '/') if prefix else ''

        s3_uri = f"s3://{self.s3_cfg.bucket}/{prefix}raw/{space}/views/{table}"

        try:
            write_deltalake(
                s3_uri,
                pa.Table.from_pylist(rows),
                mode="append",
                schema_mode="merge",
                storage_options={
                    "AWS_REGION": self.s3_cfg.region or os.getenv("AWS_REGION"),
                    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                },
            )
            tombstones = [r["externalId"] for r in rows if r.get("deletedTime")]
            if tombstones:
                dt = DeltaTable(
                    s3_uri,
                    storage_options={
                        "AWS_REGION": self.s3_cfg.region or os.getenv("AWS_REGION"),
                        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                    },
                )
                escaped = [x.replace("'", "''") for x in tombstones]
                predicate = f'externalId IN ({', '.join([f"'{xid}'" for xid in escaped])})'
                dt.delete(predicate)
        except DeltaError as err:
            self.logger.error("Delta write failed: %s", err)
            raise
