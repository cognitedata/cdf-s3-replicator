import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
from cognite.client.data_classes.data_modeling.ids import ViewId, ContainerId
from cognite.client.data_classes.data_modeling.query import (
    EdgeResultSetExpression,
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import Equals, HasData, Not, MatchAll
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

        while not self.stop_event.is_set():
            try:
                t0 = time.time()
                self.process_spaces()
                for dm_cfg in self.config.data_modeling:
                 self._publish_space_snapshots(dm_cfg)
                delay = max(self.config.extractor.poll_time - (time.time() - t0), 0)
                if delay:
                    self.stop_event.wait(delay)
            except Exception as err:
                self.logger.exception("Snapshot publish failed for %s: %s", dm_cfg.space, err)


    def process_spaces(self) -> None:
        """
        Processes all configured data modeling spaces.
        Iterates over views in each space and processes both node and edge instances.
        """
        for dm_cfg in self.config.data_modeling:
            try:
                all_views = self.cognite_client.data_modeling.views.list(
                    space=dm_cfg.space, limit=-1
                )
            except CogniteAPIError as err:
                self.logger.error("View-list failed for %s: %s", dm_cfg.space, err)
                continue

            for v in all_views.dump():
                self._process_instances(dm_cfg, f"{dm_cfg.space}_{v['externalId']}_{v['version']}", v)

            self._process_instances(dm_cfg, f"{dm_cfg.space}_edges")

    def _process_instances(
        self,
        dm_cfg: DataModelingConfig,
        state_id: str,
        view: Dict[str, Any] | None = None,
    ) -> None:
        """
        Processes either a node or edge view and writes its instances.
        Determines the correct query and delegates processing to `_iterate_and_write`.
        """
        query = (
            self._q_for_view(view) if view else self._q_for_edge()
        )
        self._iterate_and_write(dm_cfg, state_id, query)

    @staticmethod
    def _q_for_view(view: Dict[str, Any]) -> Query:
        """
        Constructs a query for a node or edge view based on the view metadata.
        Returns a node query unless the view is of type 'edge'.
        """
        props = list(view["properties"])
        view_space = view["space"]
        view_external_id = view["externalId"]
        vid = ViewId(view["space"], view_external_id, view["version"])
        if view["usedFor"] != "edge":
            query = Query(
                with_={
                    "nodes": NodeResultSetExpression(filter=HasData(views=[vid])),
                },
                select={
                    "nodes": Select(
                        [SourceSelector(source=vid, properties=props)]
                    ),
                },
            )
            return query
        return Query(
            with_={
                "edges": EdgeResultSetExpression(
                    filter=Equals(["edge", "type"], {"space": vid.space, "externalId": vid.external_id})
                )
            },
            select={"edges": Select([SourceSelector(vid, props)])},
        )

    @staticmethod
    def _q_for_edge() -> Query:
        """
        Constructs a query for all edge instances with no filters.
        Typically used to retrieve all edges in a space.
        """
        return Query(with_={"edges": EdgeResultSetExpression()}, select={"edges": Select()})

    def _iterate_and_write(self, dm_cfg: DataModelingConfig, state_id: str, query: Query) -> None:
        """
        Executes a paginated query and writes results to local Delta tables and S3.
        Maintains cursor state and handles failures gracefully with retry logic.
        """
        cursors = self.state_store.get_state(external_id=state_id)[1]
        if cursors:
            query.cursors = json.loads(str(cursors))
        elif state_id.endswith("_edges"):
            query.with_ = {"edges": EdgeResultSetExpression(filter=Not(MatchAll()))}
        else:
            query.with_ = {"nodes": NodeResultSetExpression(filter=Not(MatchAll()))}

        try:
            res = self.cognite_client.data_modeling.instances.sync(query=query)
        except CogniteAPIError:
            query.cursors = None  # type: ignore
            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError as e:
                self.logger.error(f"Failed to sync instances. Error: {e}")
                raise e

        self._send_to_s3(data_model_config=dm_cfg, result=res)
        while ("nodes" in res.data and len(res.data["nodes"]) > 0) or (
                "edges" in res.data and len(res.data["edges"])
        ) > 0:
            query.cursors = res.cursors

            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError:
                query.cursors = None  # type: ignore
                try:
                    res = self.cognite_client.data_modeling.instances.sync(query=query)
                except CogniteAPIError as e:
                    self.logger.error(f"Failed to sync instances. Error: {e}")
                    raise e

            self._send_to_s3(data_model_config=dm_cfg, result=res)

        if cursors is None:
            if "nodes" in query.select:
                sources = query.select["nodes"].sources
                instance_type = "node"
            elif "edges" in query.select:
                sources = query.select["edges"].sources
                instance_type = "edge"

            for chunk in self.cognite_client.data_modeling.instances(
                    sources=sources[0].source if sources else None,
                    instance_type=instance_type,
                    chunk_size=int(self.config.extractor.fabric_ingest_batch_size),
            ):
                if chunk:
                    self._send_to_s3(data_model_config=dm_cfg, result=res)

        self.state_store.set_state(external_id=state_id, high=json.dumps(query.cursors))
        self.state_store.synchronize()

    def _raw_prefix(self, dm_space: str) -> str:
        """
        Returns the S3 URI prefix for raw data in the given data modeling space.
        Example: s3://bucket/prefix/raw/<space>
        """
        return f"s3://{self.s3_cfg.bucket}/{self.s3_cfg.prefix.rstrip('/')}/raw/{dm_space}"

    def _pub_prefix(self, dm_space: str) -> str:
        """
        Returns the S3 URI prefix for published snapshot data in the given space.
        Example: s3://bucket/prefix/publish/<space>
        """
        return f"s3://{self.s3_cfg.bucket}/{self.s3_cfg.prefix.rstrip('/')}/publish/{dm_space}"

    def _current_views_map(self, dm_space: str) -> Dict[str, Dict[str, Union[int, bool]]]:
        """
        Returns {externalId: {"version": int, "is_edge": bool}}
        The Views API already guarantees each externalId appears once,
        representing its latest version.
        """
        views = self.cognite_client.data_modeling.views.list(space=dm_space, limit=-1)
        mapping: Dict[str, Dict[str, Union[int, bool]]] = {
            v.external_id: {"version": v.version, "is_edge": v.used_for == "edge"}
            for v in views
        }
        mapping["_edges"] = {"version": None, "is_edge": True}
        return mapping

    def _publish_space_snapshots(self, dm_cfg: DataModelingConfig) -> None:
        """
        Publishes the latest snapshot for each view and edge in a DM space.
        Loads Delta tables and writes Parquet snapshots to S3.
        """
        dm_space = dm_cfg.space
        self.logger.info("⏩  Publishing snapshots for DM space '%s'...", dm_space)

        view_map = self._current_views_map(dm_space)
        for ext_id, meta in view_map.items():
            if ext_id == "_edges":
                continue
            self._write_snapshot(
                dm_space,
                ext_id,
                meta["version"],
                meta["is_edge"],
            )

        self._write_snapshot(dm_space, "_edges", None, True)

    def _write_snapshot(
            self,
            dm_space: str,
            ext_id: str,
            version: Optional[int],
            is_edge: bool,
    ) -> None:
        """
        Writes a snapshot Parquet file for a given view or edge set.
        Reads the Delta table, optionally deduplicates nodes, and writes to S3.
        """
        raw_prefix = self._raw_prefix(dm_space)
        pub_prefix = self._pub_prefix(dm_space)

        if is_edge:
            raw_uri = f"{raw_prefix}/_edges"
            pub_uri = f"{pub_prefix}/edges/"
        else:
            raw_uri = f"{raw_prefix}/{ext_id}_v{version}"
            pub_uri = f"{pub_prefix}/{ext_id}/"

        try:
            dt = DeltaTable(raw_uri)
            tbl = dt.to_pyarrow_table()
        except FileNotFoundError:
            self.logger.warning("No data yet for %s — skipping.", raw_uri)
            return

        if not is_edge and tbl.num_rows:
            pdf = (
                tbl.to_pandas()
                .sort_values("lastUpdatedTime", ascending=False)
                .drop_duplicates(subset=["space", "externalId", "instanceType"], keep="first")
            )
            tbl = pa.Table.from_pandas(pdf, preserve_index=False)

        pq.write_table(
            tbl,
            f"{pub_uri}part-00000.parquet",
            compression="snappy",
        )
        self.logger.info("📤  Snapshot written → %s (rows=%s)", pub_uri, tbl.num_rows)


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
    def _extract_instances(res: QueryResult) -> Dict[str, List[Dict[str, Union[str, int]]]]:
        """
        Extracts and flattens node and edge instances from a QueryResult.
        Returns a dictionary mapping table names to lists of property dictionaries.
        """
        instances: Dict[str, List[Dict[str, Union[str, int]]]] = {}
        for edges in res.data.get("edges", []):
            tbl = f"{edges.space}_edges"
            instances.setdefault(tbl, []).append(
                {
                    "space":          edges.space,
                    "instanceType":   "edge",
                    "externalId":     edges.external_id,
                    "version":        edges.version,
                    "startNode.space":        edges.start_node.space,
                    "startNode.externalId":   edges.start_node.external_id,
                    "endNode.space":          edges.end_node.space,
                    "endNode.externalId":     edges.end_node.external_id,
                    "lastUpdatedTime": edges.last_updated_time,
                    "createdTime":     edges.created_time,
                    **{k: v for p in edges.properties.data.values() for k, v in p.items()},
                }
            )

        for nodes in res.data.get("nodes", []):
            for view_id, props in nodes.properties.data.items():
                tbl = f"{view_id.space}_{view_id.external_id}"
                instances.setdefault(tbl, []).append(
                    {
                        "space":           nodes.space,
                        "instanceType":    "node",
                        "externalId":      nodes.external_id,
                        "version":         nodes.version,
                        "lastUpdatedTime": nodes.last_updated_time,
                        "createdTime":     nodes.created_time,
                        **props,
                    }
                )

        return instances


    def _delta_append(self, table: str, rows: List[Dict[str, Any]], space: str) -> None:
        """
        Appends instance data rows to a Delta table, either on S3 or local.
        Handles Delta table creation and write failures gracefully.
        """
        uri = (
            f"s3://{self.s3_cfg.bucket}/"
            f"{(self.s3_cfg.prefix or '')}{space}/tables/{table}"
        )
        self.logger.info("Δ-append %s rows → %s", len(rows), uri)

        try:
            uri = self.base_dir / space / "tables" / table
            uri.mkdir(parents=True, exist_ok=True)

            self.logger.info("Δ‑append %s rows → %s", len(rows), uri)
            write_deltalake(
                uri,
                pa.Table.from_pylist(rows),
                mode='append',
                storage_options={
                    'AWS_REGION': self.s3_cfg.region or os.getenv("AWS_REGION"),
                    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
                    'AWS_SECRET_ACCESS_KEY': os.getenv("AWS_SECRET_ACCESS_KEY"),
                },
            )
        except DeltaError as err:
            self.logger.error("Delta write failed: %s", err)
            raise
