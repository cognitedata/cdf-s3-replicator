import json
import logging
import os
import time
import boto3
from pathlib import Path
import uuid
from typing import Any, Dict, Optional, Union
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
from cognite.client.data_classes.filters import Equals, HasData, Or
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import CancellationToken, Extractor
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
import pyarrow.parquet as pq

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, DataModelingConfig
from cdf_fabric_replicator.metrics import Metrics

from cdf_fabric_replicator.extractor_config import CdfExtractorConfig


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
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        self.s3_cfg = None
        self._model_xid: str | None = None
        self._model_version: str | None = None
        self.base_dir: Path = Path.cwd() / "deltalake"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
        self._s3 = None
        self.LARGE_TABLE_THRESHOLD = 1_000_000  # Threshold for large views to optimize memory usage


    def _ensure_s3(self):
        """Adds S3 client if not already initialized."""
        if self._s3 is None:
            required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION']
            missing = [var for var in required_vars if not os.getenv(var)]
            if missing:
                raise RuntimeError(f"Missing required environment variables: {missing}")

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

        try:
            extraction_pipeline = self.config.cognite.get_extraction_pipeline(self.cognite_client)
            if extraction_pipeline is None:
                self.logger.info("No extraction pipeline configured — exiting.")
                return
        except Exception as e:
            self.logger.error(f"Failed to get extraction pipeline: {e}")
            return

        while not self.stop_event.is_set():
            t0 = time.time()

            try:
                remote_config = self._reload_remote_config()
                if remote_config:
                    self.logger.info("Configuration reloaded - processing with updated config")
            except Exception as e:
                self.logger.error(f"Error during config reload: {e}")

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

            delay = max(self.config.extractor.poll_time - (time.time() - t0), 0)
            if delay:
                self.logger.info(
                    "Cycle finished in %.1fs – sleeping %ds",
                    time.time() - t0,
                    int(delay),
                )
                self.stop_event.wait(delay)


    def process_spaces(self) -> None:
        """
        Replicate the views configured in `config.yaml`, writing them under
            raw/<space>/<model-xid>/<version>/views/…
            publish/<space>/<model-xid>/<version>/views…
        Always uses explicit data model version numbers.
        """
        for dm_cfg in self.config.data_modeling:
            if self.stop_event.is_set():
                self.logger.info("Stop requested during space processing")
                return

            if dm_cfg.data_models:
                for model in dm_cfg.data_models:
                    self._model_xid = model.external_id
                    self._model_version = None

                    try:
                        if model.version is not None:
                            self._model_version = str(model.version)
                            data_model_id = (dm_cfg.space, model.external_id, model.version)
                            data_models = self.cognite_client.data_modeling.data_models.retrieve(
                                ids=[data_model_id],
                                inline_views=False
                            )
                            if not data_models:
                                self.logger.warning(
                                    "Data model %s version %s not found in space %s",
                                    model.external_id, model.version, dm_cfg.space
                                )
                                continue

                            data_model = data_models[0]
                            if not data_model.views:
                                self.logger.warning(
                                    "Data model %s version %s has no views in space %s",
                                    model.external_id, model.version, dm_cfg.space
                                )
                                continue

                            view_ids = data_model.views
                            wanted = set(model.views) if model.views else None
                            if wanted is not None:
                                view_ids = [v for v in view_ids if v.external_id in wanted]
                                found_views = {v.external_id for v in view_ids}
                                missing_views = wanted - found_views
                                if missing_views:
                                    self.logger.warning(
                                        "Views %s not found in data model %s version %s",
                                        ", ".join(sorted(missing_views)), model.external_id, model.version
                                    )

                            if view_ids:
                                view_tuples = [(v.space, v.external_id, v.version) for v in view_ids]
                                selected_views = self.cognite_client.data_modeling.views.retrieve(
                                    ids=view_tuples,
                                )
                            else:
                                selected_views = []
                        else:
                            all_data_models = self.cognite_client.data_modeling.data_models.list(
                                space=dm_cfg.space,
                                limit=-1,
                                all_versions=False
                            )
                            latest_model = next(
                                (dm for dm in all_data_models if dm.external_id == model.external_id),
                                None
                            )
                            if not latest_model:
                                self.logger.error(
                                    "No data model found with external_id %s in space %s",
                                    model.external_id, dm_cfg.space
                                )
                                continue

                            self._model_version = str(latest_model.version)
                            if latest_model.views:
                                view_ids = latest_model.views
                                wanted = set(model.views) if model.views else None

                                if wanted is not None:
                                    view_ids = [v for v in view_ids if v.external_id in wanted]

                                    found_views = {v.external_id for v in view_ids}
                                    missing_views = wanted - found_views
                                    if missing_views:
                                        self.logger.warning(
                                            "Views %s not found in data model %s version %s",
                                            ", ".join(sorted(missing_views)), model.external_id, self._model_version
                                        )

                                if view_ids:
                                    view_tuples = [(v.space, v.external_id, v.version) for v in view_ids]
                                    selected_views = self.cognite_client.data_modeling.views.retrieve(
                                        ids=view_tuples,
                                    )
                                else:
                                    selected_views = []
                            else:
                                self.logger.error(
                                    "Data model %s version %s has no views defined - cannot proceed",
                                    model.external_id, self._model_version
                                )
                                continue

                        if not self._model_version:
                            raise RuntimeError(
                                f"CRITICAL: _model_version not set for {model.external_id} in space {dm_cfg.space}"
                            )

                        if not selected_views:
                            self.logger.warning(
                                "No matching views found for model %s version %s in space %s",
                                model.external_id, self._model_version, dm_cfg.space,
                            )
                            continue

                        for view in selected_views:
                            try:
                                self.replicate_view(dm_cfg, model.external_id, self._model_version, view.dump())
                            except Exception as exc:
                                self.logger.error(
                                    "Replicating %s.%s (v%s) for model %s version %s failed: %s",
                                    dm_cfg.space, view.external_id, view.version,
                                    model.external_id, self._model_version, exc,
                                )
                    except Exception as err:
                        self.logger.error(
                            "Failed to process data model %s in space %s: %s",
                            model.external_id, dm_cfg.space, err, exc_info=True
                        )
                    finally:
                        self.logger.info(
                            "=== Finished processing data model %s version %s ===",
                            model.external_id, self._model_version or "UNKNOWN"
                        )
                        self._model_xid = None
                        self._model_version = None


    def replicate_view(self, dm_cfg: DataModelingConfig, dm_external_id: str, dm_version: str,  view: dict[str, Any]) -> None:
        """Two separate syncs to keep each query small."""
        if view.get("usedFor") != "edge":
            self._process_instances(
                dm_cfg,
                f"{view['space']}_{dm_external_id}_{dm_version}_{view['externalId']}_nodes",
                view=view,
                kind="nodes",
            )

        self._process_instances(
            dm_cfg,
            f"{view['space']}_{dm_external_id}_{dm_version}_{view['externalId']}_edges",
            view=view,
            kind="edges",
        )


    def _process_instances(self, dm_cfg, state_id, view, kind="nodes"):
        query = (
            self._edge_query_for_view(dm_cfg, view) if kind == "edges"
            else self._node_query_for_view(dm_cfg, view)
        )
        self._iterate_and_write(dm_cfg, state_id, query)


    def _node_query_for_view(self, dm_cfg: DataModelingConfig, view: dict[str, Any]) -> Query:
        vid = ViewId(view["space"], view["externalId"], view["version"])
        props = list(view["properties"])
        container_pred = HasData(
            containers=[ContainerId(dm_cfg.space, view["externalId"])]
        )
        view_pred = HasData(views=[ViewId(view["space"],
                                          view["externalId"],
                                          view["version"])])

        node_filter = Or(container_pred, view_pred)
        with_ = {
            "nodes": NodeResultSetExpression(filter=node_filter, limit=2000)
        }
        select = {
            "nodes": Select([SourceSelector(vid, props)])
        }
        return Query(with_=with_, select=select)


    def _edge_query_for_view(self, dm_cfg: DataModelingConfig, view: dict[str, Any]) -> Query:
        vid = ViewId(view["space"], view["externalId"], view["version"])
        props = list(view["properties"])
        if view.get("usedFor") != "edge":
            container_pred = HasData(
                containers=[ContainerId(dm_cfg.space, view["externalId"])]
            )
            view_pred = HasData(views=[vid])
            node_filter = Or(container_pred, view_pred)

            anchor = NodeResultSetExpression(filter=node_filter, limit=2000)

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
                    filter=Equals(["edge", "type"],
                                  {"space": vid.space, "externalId": vid.external_id}),
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
        Safely sync CDF data modeling instances to S3 Delta tables with enhanced error handling.
        This method performs incremental sync using cursors to track progress and includes multiple
        safety mechanisms to prevent data loss during API failures or temporary outages.
        """
        cursors = self.state_store.get_state(external_id=state_id)[1]

        if cursors:
            query.cursors = json.loads(str(cursors))

        original_cursors = query.cursors

        try:
            res = self.cognite_client.data_modeling.instances.sync(
                query=query,
                include_typing=False
            )
        except CogniteAPIError as e:
            self.logger.warning(f"Initial sync failed for {state_id}: {e}. Retrying with original cursors...")
            query.cursors = original_cursors
            try:
                res = self.cognite_client.data_modeling.instances.sync(
                    query=query,
                    include_typing=False
                )
            except CogniteAPIError as e:
                self.logger.error(f"Retry sync also failed for {state_id}: {e}")
                if original_cursors is not None:
                    self.logger.warning(f"Resetting cursors for {state_id} as last resort")
                    query.cursors = None
                    res = self.cognite_client.data_modeling.instances.sync(
                        query=query,
                        include_typing=False
                    )
                else:
                    raise e

        query_start_ms = int(time.time() * 1000)
        has_data = any(len(res.data.get(k, [])) > 0 for k in ("nodes", "edges", "edges_out", "edges_in"))
        if has_data:
            self._send_to_s3(data_model_config=dm_cfg, result=res)
        else:
            self.logger.info(f"No new data for {state_id} - skipping write")

        page_count = 1
        while any(len(res.data.get(k, [])) > 0 for k in ("nodes", "edges", "edges_out", "edges_in")):
            if self._page_contains_future_change(res, query_start_ms):
                self.logger.info(
                    f"Short-circuiting {state_id} on page {page_count} – instance updated after paging started"
                )
                break

            query.cursors = res.cursors
            try:
                res = self.cognite_client.data_modeling.instances.sync(
                    query=query,
                    include_typing=False
                )
                page_count += 1
            except CogniteAPIError as e:
                self.logger.warning(f"Page {page_count} failed for {state_id}: {e}. Keeping current cursors...")
                break

            has_page_data = any(len(res.data.get(k, [])) > 0 for k in ("nodes", "edges", "edges_out", "edges_in"))
            if has_page_data:
                self._send_to_s3(data_model_config=dm_cfg, result=res)

        final_cursors = query.cursors
        if final_cursors:
            self.state_store.set_state(external_id=state_id, high=json.dumps(final_cursors))
            self.state_store.synchronize()
        else:
            self.logger.warning(f"Not updating state for {state_id} - no valid cursors")


    def _raw_prefix(self, dm_space: str) -> str:
        """
        s3://<bucket>/<prefix>/raw/<space>/<model-xid>/<version>/views
        """
        model = self._model_xid or "default"
        version = self._model_version
        if not version:
            error_msg = f"CRITICAL ERROR: No data model version set for model {model} in space {dm_space}. This should never happen!"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        prefix = (self.s3_cfg.prefix.rstrip('/') + '/') if self.s3_cfg.prefix else ''
        s3_path = f"s3://{self.s3_cfg.bucket}/{prefix}raw/{dm_space}/{model}/{version}"
        return s3_path


    def _publish_prefix(self, dm_space: str) -> str:
        """
        s3://<bucket>/<prefix>/publish/<space>/<model-xid>/<version>/views
        """
        model = self._model_xid or "default"
        version = self._model_version
        if not version:
            error_msg = f"CRITICAL ERROR: No data model version set for model {model} in space {dm_space}. This should never happen!"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        prefix = (self.s3_cfg.prefix.rstrip('/') + '/') if self.s3_cfg.prefix else ''
        s3_path = f"s3://{self.s3_cfg.bucket}/{prefix}publish/{dm_space}/{model}/{version}"
        return s3_path


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
        Write Tableau-friendly snapshots under publish/<space>/<model-xid>/<version>/views
        Always uses explicit data model version numbers.
        """
        dm_space = dm_cfg.space

        if dm_cfg.data_models:
            for model in dm_cfg.data_models:
                self._model_xid = model.external_id
                self._model_version = None
                try:
                    view_map = {}

                    if model.version is not None:
                        self._model_version = str(model.version)
                        data_model_id = (dm_space, model.external_id, model.version)
                        data_models = self.cognite_client.data_modeling.data_models.retrieve(
                            ids=[data_model_id],
                            inline_views=False
                        )

                        if not data_models:
                            self.logger.warning(
                                "Data model %s version %s not found in space %s during snapshot",
                                model.external_id, model.version, dm_space
                            )
                            continue

                        data_model = data_models[0]
                        if not data_model.views:
                            self.logger.warning(
                                "Data model %s version %s has no views in space %s during snapshot",
                                model.external_id, model.version, dm_space
                            )
                            continue

                        view_ids = data_model.views
                        wanted = set(model.views) if model.views else None
                        if wanted is not None:
                            view_ids = [v for v in view_ids if v.external_id in wanted]

                            found_views = {v.external_id for v in view_ids}
                            missing_views = wanted - found_views
                            if missing_views:
                                self.logger.warning(
                                    "Views %s not found in data model %s version %s during snapshot",
                                    ", ".join(sorted(missing_views)), model.external_id, model.version
                                )

                        if view_ids:
                            view_tuples = [(v.space, v.external_id, v.version) for v in view_ids]
                            selected_views = self.cognite_client.data_modeling.views.retrieve(
                                ids=view_tuples,
                            )
                            for v in selected_views:
                                view_map[v.external_id] = {
                                    "version": v.version,
                                    "is_edge": v.used_for == "edge"
                                }

                    else:
                        all_data_models = self.cognite_client.data_modeling.data_models.list(
                            space=dm_space,
                            limit=-1,
                            all_versions=False
                        )
                        latest_model = next(
                            (dm for dm in all_data_models if dm.external_id == model.external_id),
                            None
                        )
                        if not latest_model:
                            self.logger.error(
                                "No data model found with external_id %s in space %s",
                                model.external_id, dm_cfg.space
                            )
                            continue

                        self._model_version = str(latest_model.version)
                        if latest_model.views:
                            view_ids = latest_model.views
                            wanted = set(model.views) if model.views else None

                            if wanted is not None:
                                view_ids = [v for v in view_ids if v.external_id in wanted]
                                found_views = {v.external_id for v in view_ids}
                                missing_views = wanted - found_views
                                if missing_views:
                                    self.logger.warning(
                                        "Views %s not found in data model %s version %s during snapshot",
                                        ", ".join(sorted(missing_views)), model.external_id, self._model_version
                                    )

                            if view_ids:
                                view_tuples = [(v.space, v.external_id, v.version) for v in view_ids]
                                selected_views = self.cognite_client.data_modeling.views.retrieve(
                                    ids=view_tuples,
                                )

                                for v in selected_views:
                                    view_map[v.external_id] = {
                                        "version": v.version,
                                        "is_edge": v.used_for == "edge"
                                    }
                        else:
                            self.logger.error(
                                "Data model %s version %s has no views defined - cannot create snapshots",
                                model.external_id, self._model_version
                            )
                            continue

                    if not self._model_version:
                        raise RuntimeError(
                            f"CRITICAL: _model_version not set for {model.external_id} in space {dm_space}"
                        )

                    view_map["_edges"] = {"version": None, "is_edge": True}
                    for xid, meta in view_map.items():
                        try:
                            self._write_view_snapshot(
                                dm_space, xid, meta["is_edge"]
                            )
                        except Exception as exc:
                            version_info = f" (v{meta['version']})" if meta["version"] else ""
                            self.logger.exception(
                                "Snapshot publish failed for %s.%s%s in model %s version %s: %s",
                                dm_space, xid, version_info, model.external_id, self._model_version, exc,
                            )
                except Exception as err:
                    self.logger.error(
                        "Failed to create snapshots for data model %s in space %s: %s",
                        model.external_id, dm_space, err, exc_info=True
                    )
                finally:
                    self.logger.info(
                        "=== Finished creating snapshots for data model %s version %s ===",
                        model.external_id, self._model_version or "UNKNOWN"
                    )
                    self._model_xid = None
                    self._model_version = None

            return


    def _edge_folders_for_anchor(self, dm_space: str, anchor_xid: str) -> list[str]:
        """
        Return every s3://…/raw/<space>/<model>/<version>/views/<edgeView>/edges folder that
        (heuristically) belongs to the anchor node view *anchor_xid*.

        • The anchor's own   <anchor_xid>/edges
        • Anything that starts with   <anchor_xid>.*
        """
        self._ensure_s3()
        prefix = f"{self._raw_prefix(dm_space)}/views/"
        bucket, key_prefix = prefix[5:].split("/", 1)
        resp = self._s3.list_objects_v2(
            Bucket=bucket, Prefix=key_prefix, Delimiter="/"
        )
        folders = [
            cp["Prefix"][len(key_prefix):-1]
            for cp in resp.get("CommonPrefixes", [])
            if cp["Prefix"].endswith("/edges/")
        ]
        edge_view_folders = [
            f"{prefix}{f}"
            for f in folders
            if f.startswith(anchor_xid)
        ]
        anchor_edges = f"{prefix}{anchor_xid}/edges"
        if anchor_edges not in edge_view_folders:
            edge_view_folders.append(anchor_edges)

        return edge_view_folders


    def _write_view_snapshot(self, dm_space: str, view_xid: str, is_edge_only: bool) -> None:
        """
        Tableau-optimized stable filenames with atomic replacement.
        Always writes to: nodes.parquet and edges.parquet (never deletes directory).
        Uses temporary files to ensure Tableau never sees partial/missing data.
        Memory-efficient processing for large views.
        Reads from versioned raw paths.
        """
        pub_dir = f"{self._publish_prefix(dm_space)}/{view_xid}/"
        temp_suffix = f"_temp_{uuid.uuid4().hex[:8]}"
        files_to_update = []

        try:
            if is_edge_only:
                edge_dirs = [f"{self._raw_prefix(dm_space)}/views/_edges"]
            else:
                edge_dirs = self._edge_folders_for_anchor(dm_space, view_xid)

            edge_tables = []
            for path in edge_dirs:
                try:
                    edge_tables.append(DeltaTable(path).to_pyarrow_table())
                except (FileNotFoundError, DeltaError):
                    pass

            edge_tbl = (
                pa.concat_tables(edge_tables, promote=True)
                if edge_tables else
                self._create_empty_edges_table()
            )

            tmp_edge = f"{pub_dir}edges{temp_suffix}.parquet"
            fin_edge = f"{pub_dir}edges.parquet"
            pq.write_table(edge_tbl, tmp_edge, compression="snappy")
            files_to_update.append((tmp_edge, fin_edge, "edges"))

            if not is_edge_only:
                node_raw = f"{self._raw_prefix(dm_space)}/views/{view_xid}/nodes"
                try:
                    node_tbl = DeltaTable(node_raw).to_pyarrow_table()

                    if node_tbl.num_rows > 0:
                        if node_tbl.num_rows > self.LARGE_TABLE_THRESHOLD:
                            df = node_tbl.to_pandas(split_blocks=True, self_destruct=True)
                        else:
                            df = node_tbl.to_pandas()

                        df = (
                            df.sort_values("lastUpdatedTime", ascending=False)
                            .drop_duplicates(["space", "externalId"], keep="first")
                        )
                        node_tbl = pa.Table.from_pandas(df, preserve_index=False)

                        del df

                    temp_node_path = f"{pub_dir}nodes{temp_suffix}.parquet"
                    final_node_path = f"{pub_dir}nodes.parquet"
                    pq.write_table(node_tbl, temp_node_path, compression="snappy")
                    files_to_update.append((temp_node_path, final_node_path, "nodes"))
                except (FileNotFoundError, DeltaError):
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


    def _delta_append(self, table: str, rows: list[dict[str, Any]], space: str) -> None:
        """
        Safely append rows to S3-based Delta table with data validation and cleanup.
        This method writes CDF instance data to Delta Lake tables stored in S3, with built-in
        safety checks to prevent corruption and handle deletions (tombstone processing).
        Uses explicit data model version in the S3 path structure.
        """
        if not rows:
            return

        if rows:
            null_cols = [k for k in rows[0] if all(r.get(k) is None for r in rows)]
            if null_cols:
                for r in rows:
                    for k in null_cols:
                        r.pop(k, None)

        model = self._model_xid or "default"
        version = self._model_version
        if not version:
            raise RuntimeError(f"No data model version set for model {model} in space {space}")

        prefix = (self.s3_cfg.prefix.rstrip('/') + '/') if self.s3_cfg.prefix else ''
        s3_uri = (
            f"s3://{self.s3_cfg.bucket}/"
            f"{prefix}raw/{space}/{model}/{version}/views/{table}"
        )
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
                escaped_list = ", ".join(f"'{xid}'" for xid in escaped)
                predicate = f"externalId IN ({escaped_list})"

                dt.delete(predicate)
        except DeltaError as err:
            self.logger.error("Delta write failed: %s", err)
            raise


    def _reload_remote_config(self) -> bool:
        """Reload config from remote source if it's a remote config."""

        if not (hasattr(self.config, 'cognite') and self.config.cognite):
            self.logger.info("Not a remote config, skipping reload")
            return False

        try:
            extraction_pipeline = self.config.cognite.get_extraction_pipeline(self.cognite_client)
            if not extraction_pipeline:
                self.logger.info("No extraction pipeline found, skipping reload")
                return False
        except Exception as e:
            self.logger.error(f"Could not get extraction pipeline: {e}")
            return False

        try:
            new_config = CdfExtractorConfig.retrieve_pipeline_config(
                config=self.config,
                name=self.name,
                extraction_pipeline_external_id=extraction_pipeline.external_id
            )
            self.config = new_config
            self.s3_cfg = self.config.destination.s3 if self.config.destination else None
            return True
        except Exception as e:
            self.logger.error(f"Failed to reload remote config: {e}")
            return False
