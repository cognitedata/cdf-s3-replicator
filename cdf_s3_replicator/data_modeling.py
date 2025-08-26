import json
import logging
import os
import time
import boto3
import sqlite3
from contextlib import closing
import random
import pyarrow.dataset as ds
from pathlib import Path
import uuid
from typing import Any, Dict, Optional, Union
import pyarrow as pa
import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)
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
import pyarrow.compute as pc

from cdf_s3_replicator import __version__
from cdf_s3_replicator.config import Config, DataModelingConfig, S3DestinationConfig
from cdf_s3_replicator.metrics import Metrics

from cdf_s3_replicator.extractor_config import CdfExtractorConfig
from datetime import datetime, timedelta, timezone
from itertools import chain
import botocore.config as bc

UTC = timezone.utc


def _should_retry_exc(exc: BaseException) -> bool:
    if isinstance(exc, CogniteAPIError):
        return not (exc.code == 400 and "cursor has expired" in str(exc).lower())

    return isinstance(exc, requests.exceptions.RequestException) or isinstance(
        exc, Exception
    )


class DataModelingReplicator(Extractor):
    """Streams CDF Data-Modeling instances into S3-based Delta tables."""

    config: Config
    state_store: Any
    cognite_client: Any
    s3_cfg: Optional[S3DestinationConfig]
    _s3: Optional[boto3.client]
    _s3_created_at: Optional[datetime]
    _model_xid: Optional[str]
    _model_version: Optional[str]
    _expected_node_props: Optional[set[str]]
    _view_props_by_xid: dict[tuple[str, str], set[str]]

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
            name="cdf_s3_replicator_data_modeling",
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
        if os.getenv("APP_ENV", "dev").lower() in ("dev", "development"):
            os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
        else:
            os.environ.pop("AWS_EC2_METADATA_DISABLED", None)
        self._s3: boto3.client | None = None
        self._s3_created_at: datetime | None = None
        self._expected_node_props: set[str] | None = None
        self._view_props_by_xid: dict[tuple[str, str], set[str]] = {}

    def _is_prod(self) -> bool:
        """Return True when running in production mode."""
        return os.getenv("APP_ENV", "dev").lower() in ("prod", "production")

    def _s3_storage_options(self) -> dict[str, str]:
        """
        Build storage options for delta-rs based on runtime mode.

        Prod:
          - rely on IAM role via IMDS / container credentials (no explicit keys)
          - optionally pass region if known (helps delta-rs avoid an extra lookup)

        Dev:
          - use env keys if present (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN])
          - also pass region if set
        """
        region: Optional[str] = None
        if self.s3_cfg is not None and getattr(self.s3_cfg, "region", None):
            region = self.s3_cfg.region
        else:
            env_region = os.getenv("AWS_REGION")
            if env_region is not None:
                region = env_region

        opts: dict[str, str] = {}
        if region is not None:
            opts["AWS_REGION"] = region

        is_prod = self._is_prod()
        if not is_prod:
            akid = os.getenv("AWS_ACCESS_KEY_ID")
            skey = os.getenv("AWS_SECRET_ACCESS_KEY")

            if akid is not None and skey is not None:
                opts["AWS_ACCESS_KEY_ID"] = akid
                opts["AWS_SECRET_ACCESS_KEY"] = skey

        self.logger.info("Running in %s mode", "PROD" if is_prod else "DEV")
        return opts

    def _make_s3_client(self) -> boto3.client:
        """Create a new S3 client using the default credential chain."""
        _RETRY_CFG = bc.Config(
            retries={"max_attempts": 10, "mode": "adaptive"},
            connect_timeout=5,
            read_timeout=30,
        )
        return boto3.client("s3", config=_RETRY_CFG)

    def _ensure_s3(self) -> None:
        """Create or renew the boto3 client every ~50 min to dodge STS expiry."""
        now = datetime.now(UTC)
        if self._s3 is None or self._s3_created_at is None:
            self._s3 = self._make_s3_client()
            self._s3_created_at = now
            return
        if now - self._s3_created_at > timedelta(minutes=50):
            self._s3 = self._make_s3_client()
            self._s3_created_at = now

    def _list_prefixes(
        self, bucket: str, prefix: str, delimiter: str = "/"
    ) -> list[str]:
        """Return *all* CommonPrefixes under `prefix` (depth-1 folders)."""
        self._ensure_s3()
        assert self._s3 is not None
        paginator = self._s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
        return [
            cp["Prefix"]
            for cp in chain.from_iterable(p.get("CommonPrefixes", []) for p in pages)
        ]

    def run(self) -> None:
        """
        Main run loop for the replicator.
        Polls configured data modeling spaces and publishes snapshots at intervals until cancelled.
        """
        cfg: Config = self.config
        self.s3_cfg = cfg.destination.s3 if cfg.destination else None
        if not self.s3_cfg:
            raise RuntimeError("destination.s3 must be configured")
        self.state_store.initialize()
        if not cfg.data_modeling:
            self.logger.info("No data-modeling spaces configured — exiting.")
            return

        last_snapshot_time: float = 0.0
        snapshot_interval = float(
            getattr(
                self.config.extractor,
                "snapshot_interval",
                self.config.extractor.poll_time,
            )
        )

        try:
            extraction_pipeline = self.config.cognite.get_extraction_pipeline(
                self.cognite_client
            )
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
                    self.logger.info(
                        "Configuration reloaded - processing with updated config"
                    )
            except Exception as e:
                self.logger.error(f"Error during config reload: {e}")

            self.process_spaces()

            if t0 - last_snapshot_time >= snapshot_interval:
                for dm_cfg in cfg.data_modeling:
                    try:
                        self._publish_space_snapshots(dm_cfg)
                    except Exception as err:
                        self.logger.exception(
                            "Snapshot publish failed for %s: %s", dm_cfg.space, err
                        )
                last_snapshot_time = t0

            delay = max(cfg.extractor.poll_time - (time.time() - t0), 0)
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
        cfg: Config = self.config
        if not cfg.data_modeling:
            return

        for dm_cfg in cfg.data_modeling:
            if self.stop_event.is_set():
                self.logger.info("Stop requested during space processing")
                return

            if dm_cfg.data_models:
                for model in dm_cfg.data_models:
                    try:
                        model_version, selected_views = self._get_data_model_views(
                            dm_cfg, model
                        )

                        if not model_version:
                            continue

                        if not selected_views:
                            self.logger.warning(
                                "No matching views found for model %s version %s in space %s",
                                model.external_id,
                                model_version,
                                dm_cfg.space,
                            )
                            continue

                        for view in selected_views:
                            try:
                                self.replicate_view(
                                    dm_cfg,
                                    model.external_id,
                                    model_version,
                                    view.dump(),
                                )
                            except Exception as exc:
                                self.logger.error(
                                    "Replicating %s.%s (v%s) for model %s version %s failed: %s",
                                    dm_cfg.space,
                                    view.external_id,
                                    view.version,
                                    model.external_id,
                                    model_version,
                                    exc,
                                )
                    except Exception as err:
                        self.logger.error(
                            "Failed to process data model %s in space %s: %s",
                            model.external_id,
                            dm_cfg.space,
                            err,
                            exc_info=True,
                        )
                    finally:
                        self.logger.info(
                            "=== Finished processing data model %s version %s ===",
                            model.external_id,
                            self._model_version or "UNKNOWN",
                        )
                        self._model_xid = None
                        self._model_version = None

    def replicate_view(
        self,
        dm_cfg: DataModelingConfig,
        dm_external_id: str,
        dm_version: str,
        view: dict[str, Any],
    ) -> None:
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

    def _process_instances(
        self,
        dm_cfg: DataModelingConfig,
        state_id: str,
        view: dict[str, Any],
        kind: str = "nodes",
    ) -> None:
        if kind == "nodes":
            self._expected_node_props = set(view["properties"])
            self._view_props_by_xid[(dm_cfg.space, view["externalId"])] = set(
                view["properties"]
            )
        else:
            self._expected_node_props = None

        query = (
            self._edge_query_for_view(view)
            if kind == "edges"
            else self._node_query_for_view(view)
        )
        self._iterate_and_write(dm_cfg, state_id, query)

    def _node_query_for_view(self, view: dict[str, Any]) -> Query:
        vid = ViewId(view["space"], view["externalId"], view["version"])
        props = list(view["properties"])
        container_pred = HasData(
            containers=[ContainerId(view["space"], view["externalId"])]
        )
        view_pred = HasData(
            views=[ViewId(view["space"], view["externalId"], view["version"])]
        )

        node_filter = Or(container_pred, view_pred)
        with_ = {"nodes": NodeResultSetExpression(filter=node_filter, limit=2000)}
        select = {"nodes": Select([SourceSelector(vid, props)])}
        return Query(with_=with_, select=select)

    def _edge_query_for_view(self, view: dict[str, Any]) -> Query:
        vid = ViewId(view["space"], view["externalId"], view["version"])
        props = list(view["properties"])
        if view.get("usedFor") != "edge":
            container_pred = HasData(
                containers=[ContainerId(view["space"], view["externalId"])]
            )
            view_pred = HasData(views=[vid])
            node_filter = Or(container_pred, view_pred)

            anchor = NodeResultSetExpression(filter=node_filter, limit=2000)

            return Query(
                with_={
                    "nodes": anchor,
                    "edges_out": EdgeResultSetExpression(
                        from_="nodes", direction="outwards", limit=2000
                    ),
                    "edges_in": EdgeResultSetExpression(
                        from_="nodes", direction="inwards", limit=2000
                    ),
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
        def check(inst: Any) -> bool:
            return inst.last_updated_time >= t0_ms

        return any(
            check(i)
            for rs in ("nodes", "edges", "edges_out", "edges_in")
            for i in result.data.get(rs, [])
        )

    @retry(
        retry=retry_if_exception(_should_retry_exc),
        wait=wait_exponential_jitter(initial=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _safe_sync(self, client: Any, query: Query) -> Any:
        return client.data_modeling.instances.sync(query=query, include_typing=False)

    def _iterate_and_write(
        self, dm_cfg: DataModelingConfig, state_id: str, query: Query
    ) -> None:
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
            res = self._safe_sync(self.cognite_client, query)
        except CogniteAPIError as e:
            if e.code == 400 and "cursor has expired" in str(e).lower():
                self.logger.warning(
                    f"Cursor expired for {state_id}. Restarting sync from beginning..."
                )

                self.state_store.set_state(external_id=state_id, high=None)
                self.state_store.synchronize()

                query.cursors = None

                try:
                    res = self._safe_sync(self.cognite_client, query)
                    self.logger.info(
                        f"Successfully restarted sync for {state_id} after cursor expiration"
                    )
                except CogniteAPIError as retry_error:
                    self.logger.error(
                        f"Failed to restart sync for {state_id} after cursor expiration: {retry_error}"
                    )
                    raise retry_error
            else:
                self.logger.warning(
                    f"Initial sync failed for {state_id}: {e}. Retrying with original cursors..."
                )
                query.cursors = original_cursors
                try:
                    res = self._safe_sync(self.cognite_client, query)
                except CogniteAPIError as retry_error:
                    self.logger.error(
                        f"Retry sync also failed for {state_id}: {retry_error}"
                    )
                    if original_cursors is not None:
                        self.logger.warning(
                            f"Resetting cursors for {state_id} as last resort"
                        )
                        query.cursors = None
                        self.state_store.set_state(external_id=state_id, high=None)
                        self.state_store.synchronize()
                        res = self._safe_sync(self.cognite_client, query)
                    else:
                        raise retry_error

        query_start_ms = int(time.time() * 1000)
        has_data = any(
            len(res.data.get(k, [])) > 0
            for k in ("nodes", "edges", "edges_out", "edges_in")
        )
        if has_data:
            self._send_to_s3(data_model_config=dm_cfg, result=res)
            self.state_store.set_state(
                external_id=state_id, high=json.dumps(res.cursors)
            )
            self.state_store.synchronize()
        else:
            self.logger.info(f"No new data for {state_id} - skipping write")

        page_count = 1
        while any(
            len(res.data.get(k, [])) > 0
            for k in ("nodes", "edges", "edges_out", "edges_in")
        ):
            if self._page_contains_future_change(res, query_start_ms):
                self.logger.info(
                    f"Short-circuiting {state_id} on page {page_count} – instance updated after paging started"
                )
                break

            query.cursors = res.cursors
            try:
                res = self._safe_sync(self.cognite_client, query)
                page_count += 1
            except CogniteAPIError as e:
                if e.code == 400 and "cursor has expired" in str(e).lower():
                    self.logger.warning(
                        f"Cursor expired during pagination for {state_id} on page {page_count}. "
                        f"Need to restart entire sync. Breaking pagination loop..."
                    )
                    self.state_store.set_state(external_id=state_id, high=None)
                    self.state_store.synchronize()
                    break
                else:
                    self.logger.warning(
                        f"Page {page_count} failed for {state_id}: {e}. Keeping current cursors..."
                    )
                    break

            has_page_data = any(
                len(res.data.get(k, [])) > 0
                for k in ("nodes", "edges", "edges_out", "edges_in")
            )
            if has_page_data:
                self._send_to_s3(data_model_config=dm_cfg, result=res)
                self.state_store.set_state(
                    external_id=state_id, high=json.dumps(res.cursors)
                )
                self.state_store.synchronize()

        final_cursors = query.cursors
        if final_cursors:
            self.state_store.set_state(
                external_id=state_id, high=json.dumps(final_cursors)
            )
            self.state_store.synchronize()
        else:
            self.logger.warning(f"Not updating state for {state_id} - no valid cursors")

    def _get_data_model_views(
        self, dm_cfg: DataModelingConfig, model
    ) -> tuple[str | None, list]:
        """
        Centralized method to retrieve and validate data model views.
        Returns (model_version, selected_views) tuple.
        """
        self._model_xid = model.external_id
        self._model_version = None

        try:
            if model.version is not None:
                self._model_version = str(model.version)
                data_model_id = (dm_cfg.space, model.external_id, model.version)
                data_models = self.cognite_client.data_modeling.data_models.retrieve(
                    ids=[data_model_id], inline_views=False
                )

                if not data_models:
                    self.logger.warning(
                        "Data model %s version %s not found in space %s",
                        model.external_id,
                        model.version,
                        dm_cfg.space,
                    )
                    return self._model_version, []

                data_model = data_models[0]

            else:
                all_data_models = self.cognite_client.data_modeling.data_models.list(
                    space=dm_cfg.space, limit=-1, all_versions=False
                )
                data_model = next(
                    (
                        dm
                        for dm in all_data_models
                        if dm.external_id == model.external_id
                    ),
                    None,
                )
                if not data_model:
                    self.logger.error(
                        "No data model found with external_id %s in space %s",
                        model.external_id,
                        dm_cfg.space,
                    )
                    return None, []

                self._model_version = str(data_model.version)

            if not data_model.views:
                self.logger.warning(
                    "Data model %s version %s has no views in space %s",
                    model.external_id,
                    self._model_version,
                    dm_cfg.space,
                )
                return self._model_version, []

            view_ids = data_model.views
            wanted = set(model.views) if model.views else None

            if wanted is not None:
                view_ids = [v for v in view_ids if v.external_id in wanted]
                found_views = {v.external_id for v in view_ids}
                missing_views = wanted - found_views
                if missing_views:
                    self.logger.warning(
                        "Views %s not found in data model %s version %s",
                        ", ".join(sorted(missing_views)),
                        model.external_id,
                        self._model_version,
                    )

            if view_ids:
                view_tuples = [(v.space, v.external_id, v.version) for v in view_ids]
                selected_views = self.cognite_client.data_modeling.views.retrieve(
                    ids=view_tuples
                )
            else:
                selected_views = []

            return self._model_version, selected_views

        except Exception as err:
            self.logger.error(
                "Failed to retrieve data model %s in space %s: %s",
                model.external_id,
                dm_cfg.space,
                err,
                exc_info=True,
            )
            return None, []

    def _get_s3_prefix(self, dm_space: str, prefix_type: str = "raw") -> str:
        """
        Centralized S3 path construction for both raw and publish prefixes.
        """
        model = self._model_xid or "default"
        version = self._model_version
        if not version:
            error_msg = f"CRITICAL ERROR: No data model version set for model {model} in space {dm_space}. This should never happen!"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        assert self.s3_cfg is not None
        prefix = (self.s3_cfg.prefix.rstrip("/") + "/") if self.s3_cfg.prefix else ""
        s3_path = f"s3://{self.s3_cfg.bucket}/{prefix}{prefix_type}/{dm_space}/{model}/{version}"
        return s3_path

    def _raw_prefix(self, dm_space: str) -> str:
        return self._get_s3_prefix(dm_space, "raw")

    def _publish_prefix(self, dm_space: str) -> str:
        return self._get_s3_prefix(dm_space, "publish")

    def _current_views_map(
        self,
        dm_space: str,
        selected: set[str] | None = None,
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
                try:
                    model_version, selected_views = self._get_data_model_views(
                        dm_cfg, model
                    )

                    if not model_version:
                        continue

                    view_map = {}
                    for v in selected_views:
                        view_map[v.external_id] = {
                            "version": v.version,
                            "is_edge": v.used_for == "edge",
                        }

                    if not view_map:
                        self.logger.error(
                            "No views available for snapshots in data model %s version %s",
                            model.external_id,
                            model_version,
                        )
                        continue

                    view_map["_edges"] = {"version": None, "is_edge": True}
                    for xid, meta in view_map.items():
                        try:
                            self._write_view_snapshot(dm_space, xid, meta["is_edge"])
                        except Exception as exc:
                            version_info = (
                                f" (v{meta['version']})" if meta["version"] else ""
                            )
                            self.logger.exception(
                                "Snapshot publish failed for %s.%s%s in model %s version %s: %s",
                                dm_space,
                                xid,
                                version_info,
                                model.external_id,
                                model_version,
                                exc,
                            )
                except Exception as err:
                    self.logger.error(
                        "Failed to create snapshots for data model %s in space %s: %s",
                        model.external_id,
                        dm_space,
                        err,
                        exc_info=True,
                    )
                finally:
                    self.logger.info(
                        "=== Finished creating snapshots for data model %s version %s ===",
                        model.external_id,
                        self._model_version or "UNKNOWN",
                    )
                    self._model_xid = None
                    self._model_version = None

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
        folders = [
            cp[len(key_prefix) : -1]
            for cp in self._list_prefixes(bucket, key_prefix)
            if cp.endswith("/edges/")
        ]
        edge_view_folders = [
            f"{prefix}{f}" for f in folders if f.startswith(anchor_xid)
        ]
        anchor_edges = f"{prefix}{anchor_xid}/edges"
        if anchor_edges not in edge_view_folders:
            edge_view_folders.append(anchor_edges)

        return edge_view_folders

    def _dedup_latest_nodes(self, delta_path: str) -> pa.Table:
        """
        Keep only the newest row per (space, externalId).

        Two-pass streaming algorithm using a tiny on-disk SQLite index:
          1) Stream batches from the Delta table (as a PyArrow Dataset) to compute
             max(lastUpdatedTime) per (space, externalId).
          2) Stream again and keep only rows whose lastUpdatedTime equals that max.

        Why: Arrow batches don’t guarantee global order; dedupping within a batch can
        keep an older row if a newer one appears later. This fixes that correctness bug.

        Parameters
        ----------
        delta_path : str
            s3://.../raw/<space>/<model>/<version>/views/<view_xid>/nodes

        Returns
        -------
        pa.Table
            Deduplicated nodes.
        """
        dt = DeltaTable(delta_path, storage_options=self._s3_storage_options())
        dataset = dt.to_pyarrow_dataset()
        db_path = self.base_dir / f"dedup_{uuid.uuid4().hex}.sqlite"

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE maxlut(
                    space TEXT NOT NULL,
                    externalId TEXT NOT NULL,
                    lut INTEGER NOT NULL,
                    PRIMARY KEY(space, externalId)
                )
            """
            )

            for batch in dataset.to_batches(
                columns=["space", "externalId", "lastUpdatedTime"]
            ):
                sp = batch.column("space").to_pylist()
                ex = batch.column("externalId").to_pylist()
                lu = batch.column("lastUpdatedTime").to_pylist()
                conn.executemany(
                    "INSERT INTO maxlut(space, externalId, lut) VALUES(?,?,?) "
                    "ON CONFLICT(space, externalId) DO UPDATE SET lut=MAX(lut, excluded.lut)",
                    zip(sp, ex, lu),
                )
            conn.commit()

            kept_batches: list[pa.RecordBatch] = []
            for batch in dataset.to_batches():
                sp = batch.column("space").to_pylist()
                ex = batch.column("externalId").to_pylist()
                lu = batch.column("lastUpdatedTime").to_pylist()

                keys = list(set(zip(sp, ex)))
                if not keys:
                    continue
                placeholders = ",".join(["(?,?)"] * len(keys))
                args = [v for pair in keys for v in pair]
                rows = conn.execute(
                    f"SELECT space, externalId, lut FROM maxlut WHERE (space, externalId) IN ({placeholders})",
                    args,
                ).fetchall()
                lut_map = {(r[0], r[1]): r[2] for r in rows}

                mask = [
                    (s, x) in lut_map and t == lut_map[(s, x)]
                    for s, x, t in zip(sp, ex, lu)
                ]
                filtered = batch.filter(pa.array(mask))
                if filtered.num_rows:
                    kept_batches.append(filtered)

        try:
            os.remove(db_path)
        except Exception:
            pass

        if not kept_batches:
            return self._create_empty_nodes_table()

        return pa.Table.from_batches(kept_batches)

    def _widen_with_props(self, tbl: pa.Table, props: set[str]) -> pa.Table:
        """
        Ensure all props exist as string-typed columns (NULLs) in tbl.
        Does NOT change existing columns/types—only adds missing ones.
        """
        if not props or tbl is None:
            return tbl
        existing = {f.name for f in tbl.schema}
        to_add = [p for p in props if p not in existing]
        if not to_add:
            return tbl

        base_fields = [pa.field(f.name, f.type) for f in tbl.schema]
        base_arrays = [tbl.column(i) for i in range(tbl.num_columns)]
        n = tbl.num_rows
        extra_fields = [pa.field(c, pa.string()) for c in to_add]
        extra_arrays = [pa.array([None] * n, type=pa.string()) for _ in to_add]

        return pa.Table.from_arrays(
            base_arrays + extra_arrays, schema=pa.schema(base_fields + extra_fields)
        )

    def _write_view_snapshot(
        self, dm_space: str, view_xid: str, is_edge_only: bool
    ) -> None:
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
                    edge_tables.append(
                        DeltaTable(
                            path, storage_options=self._s3_storage_options()
                        ).to_pyarrow_table()
                    )
                except (FileNotFoundError, DeltaError):
                    pass

            edge_tbl = (
                pa.concat_tables(edge_tables, promote=True)
                if edge_tables
                else self._create_empty_edges_table()
            )

            tmp_edge = f"{pub_dir}edges{temp_suffix}.parquet"
            fin_edge = f"{pub_dir}edges.parquet"
            pq.write_table(edge_tbl, tmp_edge, compression="snappy")
            files_to_update.append((tmp_edge, fin_edge, "edges"))

            if not is_edge_only:
                node_raw = f"{self._raw_prefix(dm_space)}/views/{view_xid}/nodes"
                expected = self._view_props_by_xid.get((dm_space, view_xid), set())
                try:
                    if DeltaTable(
                        node_raw, storage_options=self._s3_storage_options()
                    ).files():
                        node_tbl = self._dedup_latest_nodes(node_raw)
                    else:
                        node_tbl = self._create_empty_nodes_table()

                    node_tbl = self._widen_with_props(node_tbl, expected)
                    temp_node_path = f"{pub_dir}nodes{temp_suffix}.parquet"
                    final_node_path = f"{pub_dir}nodes.parquet"
                    pq.write_table(node_tbl, temp_node_path, compression="snappy")
                    files_to_update.append((temp_node_path, final_node_path, "nodes"))
                except (FileNotFoundError, DeltaError):
                    empty_node_tbl = self._create_empty_nodes_table()
                    empty_node_tbl = self._widen_with_props(empty_node_tbl, expected)
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

    def _copy_and_verify(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        max_wait: int = 10,
    ) -> None:
        """
        Copy S3 object and block until the destination is visible.
        Uses single-request CopyObject for <= 5 GB. Falls back to multipart copy for larger objects.

        Notes
        -----
        • CopyObject is a single atomic action up to 5 GB; larger requires UploadPartCopy (multipart).
        • ETag equality is sufficient here as a visibility check (object materialized),
          not a cryptographic integrity check. S3 ETag is not guaranteed to be MD5.
        Raises
        ------
        RuntimeError on timeout or if multipart copy fails.
        """
        self._ensure_s3()
        assert self._s3 is not None
        head_src = self._s3.head_object(Bucket=src_bucket, Key=src_key)
        size = head_src["ContentLength"]

        if size <= 5 * 1024**3:
            resp = self._s3.copy_object(
                CopySource={"Bucket": src_bucket, "Key": src_key},
                Bucket=dst_bucket,
                Key=dst_key,
            )
            etag = resp["CopyObjectResult"]["ETag"].strip('"')
            for _ in range(max_wait):
                head = self._s3.head_object(Bucket=dst_bucket, Key=dst_key)
                if head.get("ETag", "").strip('"') == etag:
                    return
                time.sleep(1)
            raise RuntimeError(f"S3 eventual consistency timeout for {dst_key}")

        mpu = self._s3.create_multipart_upload(Bucket=dst_bucket, Key=dst_key)
        upload_id = mpu["UploadId"]
        try:
            part_size = 128 * 1024 * 1024
            parts = []
            part_number = 1
            for start in range(0, size, part_size):
                end = min(size - 1, start + part_size - 1)
                resp = self._s3.upload_part_copy(
                    Bucket=dst_bucket,
                    Key=dst_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    CopySource={"Bucket": src_bucket, "Key": src_key},
                    CopySourceRange=f"bytes={start}-{end}",
                )
                parts.append(
                    {"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": part_number}
                )
                part_number += 1

            self._s3.complete_multipart_upload(
                Bucket=dst_bucket,
                Key=dst_key,
                MultipartUpload={"Parts": parts},
                UploadId=upload_id,
            )
        except Exception:
            self._s3.abort_multipart_upload(
                Bucket=dst_bucket, Key=dst_key, UploadId=upload_id
            )
            raise

    def _atomic_replace_files(self, file_updates: list[tuple[str, str, str]]) -> None:
        """
        Atomically replace multiple files by copying temp files to final locations.
        Args:
            file_updates: List of (temp_path, final_path, description) tuples
        """
        self._ensure_s3()
        assert self._s3 is not None
        try:
            for temp_path, final_path, desc in file_updates:
                temp_bucket, temp_key = temp_path[5:].split("/", 1)
                final_bucket, final_key = final_path[5:].split("/", 1)
                self._copy_and_verify(temp_bucket, temp_key, final_bucket, final_key)
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
            assert self._s3 is not None
            response = self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            temp_objects = [
                obj for obj in response.get("Contents", []) if temp_suffix in obj["Key"]
            ]
            if temp_objects:
                delete_objects = [{"Key": obj["Key"]} for obj in temp_objects]
                self._s3.delete_objects(
                    Bucket=bucket, Delete={"Objects": delete_objects}
                )
        except Exception as e:
            self.logger.warning("Failed to cleanup temp files: %s", e)

    def _create_empty_edges_table(self) -> pa.Table:
        """
        Create an empty edges table with the correct schema for Tableau consistency.
        This ensures Tableau always has a file to connect to, even with 0 rows.
        """
        schema = pa.schema(
            [
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
            ]
        )
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema
        )

    def _create_empty_nodes_table(self) -> pa.Table:
        """
        Create an empty nodes table with the correct schema for Tableau consistency.
        This ensures Tableau always has a file to connect to, even with 0 rows.
        """
        schema = pa.schema(
            [
                ("space", pa.string()),
                ("instanceType", pa.string()),
                ("externalId", pa.string()),
                ("version", pa.int64()),
                ("lastUpdatedTime", pa.int64()),
                ("createdTime", pa.int64()),
                ("deletedTime", pa.int64()),
            ]
        )
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema
        )

    def _send_to_s3(
        self, data_model_config: DataModelingConfig, result: QueryResult
    ) -> None:
        """
        Extracts instance data and appends it to the corresponding S3 Delta table.
        Used during sync to continuously update raw Delta tables.
        """
        for tbl_name, rows in self._extract_instances(result).items():
            self._delta_append(tbl_name, rows, data_model_config.space)

    def _send_to_local(
        self, data_model_config: DataModelingConfig, result: QueryResult
    ) -> None:
        """
        Extracts instance data and appends it to a local Delta table.
        Useful for local development or testing instead of writing to S3.
        """
        for tbl_name, rows in self._extract_instances(result).items():
            self._delta_append(tbl_name, rows, data_model_config.space)

    def _extract_instances(self, res: QueryResult) -> dict[str, list[dict]]:
        """
        •  Edge-only views  → table "_edges"
        •  Node views       → tables "<view>/nodes" and "<view>/edges"
        """
        out: dict[str, list[dict]] = {}
        for rs_name in ("edges", "edges_out", "edges_in"):
            for e in res.data.get(rs_name, []):
                edge_view_xid = e.type.external_id
                is_edge_only = rs_name == "edges"
                tbl_name = "_edges" if is_edge_only else f"{edge_view_xid}/edges"
                direction = (
                    None
                    if rs_name == "edges"
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
                row = {
                    "space": n.space,
                    "instanceType": "node",
                    "externalId": n.external_id,
                    "version": n.version,
                    "lastUpdatedTime": n.last_updated_time,
                    "createdTime": n.created_time,
                    "deletedTime": n.deleted_time,
                    **props,
                }

                if self._expected_node_props:
                    for p in self._expected_node_props:
                        row.setdefault(p, None)

                out.setdefault(tbl_name, []).append(row)

        return out

    def _sanitize_rows_for_tableau(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Sanitize row data to ensure Tableau/Hyper compatibility.
        Converts problematic data types to safe alternatives.
        """
        if not rows:
            return rows

        column_types: dict[str, set[str]] = {}
        non_null_cols: set[str] = set()
        sanitized_rows: list[dict[str, Any]] = []

        for row in rows:
            sanitized_row = {}
            for key, value in row.items():
                if value is None:
                    sanitized_value = None
                elif isinstance(value, (dict, list)):
                    sanitized_value = json.dumps(value) if value else None
                elif isinstance(value, bytes):
                    sanitized_value = value.decode("utf-8", errors="replace")
                elif hasattr(value, "__dict__") and not isinstance(
                    value, (str, int, float, bool)
                ):
                    sanitized_value = str(value)
                else:
                    sanitized_value = value

                if sanitized_value is not None:
                    non_null_cols.add(key)
                    value_type = type(sanitized_value).__name__
                    if key not in column_types:
                        column_types[key] = set()
                    column_types[key].add(value_type)

                sanitized_row[key] = sanitized_value

            sanitized_rows.append(sanitized_row)

        core_fields = {
            "space",
            "instanceType",
            "externalId",
            "version",
            "lastUpdatedTime",
            "createdTime",
            "deletedTime",
            "startNode.space",
            "startNode.externalId",
            "endNode.space",
            "endNode.externalId",
            "direction",
        }

        mixed_type_columns: dict[str, set[str]] = {}
        for col, types in column_types.items():
            if len(types) > 1 and col not in core_fields:
                mixed_type_columns[col] = types
                self.logger.warning(
                    f"Mixed types in column {col}: {types}. Converting to string."
                )

        if mixed_type_columns:
            for row in sanitized_rows:
                for col in mixed_type_columns:
                    if row.get(col) is not None:
                        row[col] = str(row[col])

        return sanitized_rows

    def _validate_tableau_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Check for known problematic types that Tableau can't handle."""
        problematic_types = []

        for field in schema:
            field_type = field.type

            is_problematic = (
                field_type in (pa.binary(), pa.large_binary())
                or field_type == pa.null()
                or str(field_type).startswith("extension")
                or str(field_type).startswith("union")
            )

            try:
                logical_type = getattr(field_type, "logical_type", None)
                if logical_type and hasattr(logical_type, "__dict__"):
                    logical_dict = (
                        logical_type.__dict__
                        if hasattr(logical_type, "__dict__")
                        else {}
                    )
                    if (
                        "UNKNOWN" in str(logical_dict)
                        or "unknown" in str(logical_dict).lower()
                    ):
                        is_problematic = True
            except Exception:
                if "UNKNOWN" in str(field_type):
                    is_problematic = True

            if is_problematic:
                problematic_types.append(f"{field.name}: {field_type}")

        if problematic_types:
            raise ValueError(f"Tableau-incompatible types found: {problematic_types}")

    def _convert_to_safe_types(self, rows: list[dict[str, Any]]) -> pa.Table:
        """Last resort: convert everything except core fields to strings."""
        if not rows:
            return pa.Table.from_pylist([])

        core_fields = {
            "space": pa.string(),
            "instanceType": pa.string(),
            "externalId": pa.string(),
            "version": pa.int64(),
            "lastUpdatedTime": pa.int64(),
            "createdTime": pa.int64(),
            "deletedTime": pa.int64(),
            "startNode.space": pa.string(),
            "startNode.externalId": pa.string(),
            "endNode.space": pa.string(),
            "endNode.externalId": pa.string(),
            "direction": pa.string(),
        }

        all_columns: set[str] = set()
        for row in rows:
            all_columns.update(row.keys())

        schema_fields = []
        for col in sorted(all_columns):
            if col in core_fields:
                schema_fields.append(pa.field(col, core_fields[col]))
            else:
                schema_fields.append(pa.field(col, pa.string()))

        safe_schema = pa.schema(schema_fields)

        safe_rows: list[dict[str, Any]] = []
        for row in rows:
            safe_row: dict[str, Any] = {}
            for field in safe_schema:
                col_name = field.name
                value = row.get(col_name)

                if value is None:
                    safe_row[col_name] = None
                elif field.type == pa.string():
                    safe_row[col_name] = str(value) if value is not None else None
                elif field.type == pa.int64():
                    try:
                        safe_row[col_name] = int(value) if value is not None else None
                    except (ValueError, TypeError):
                        safe_row[col_name] = None
                else:
                    safe_row[col_name] = value

            safe_rows.append(safe_row)

        return pa.Table.from_pylist(safe_rows, schema=safe_schema)

    def _delete_tombstones(
        self, dt: DeltaTable, tombstones: list[str], chunk: int = 5_000
    ) -> None:
        for i in range(0, len(tombstones), chunk):
            part = tombstones[i : i + chunk]
            escaped = [x.replace("'", "''") for x in part]
            escaped_list = ", ".join(f"'{xid}'" for xid in escaped)
            predicate = f"externalId IN ({escaped_list})"
            dt.delete(predicate)

    def _coerce_nulltype_fields_to_string(self, tbl: pa.Table) -> pa.Table:
        """Turn any pa.null() columns into string-typed null columns, leave everything else as-is."""
        if tbl is None or tbl.num_columns == 0:
            return tbl
        if not any(pa.types.is_null(f.type) for f in tbl.schema):
            return tbl

        arrays, fields = [], []
        n = tbl.num_rows
        for f in tbl.schema:
            col = tbl.column(f.name)
            if pa.types.is_null(f.type):
                fields.append(pa.field(f.name, pa.string()))
                arrays.append(pa.array([None] * n, type=pa.string()))
            else:
                fields.append(pa.field(f.name, f.type))
                arrays.append(col)
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _align_to_existing_schema(self, s3_uri: str, tbl: pa.Table) -> pa.Table:
        try:
            dt = DeltaTable(s3_uri, storage_options=self._s3_storage_options())
        except Exception:
            return tbl

        try:
            existing = dt.schema().to_pyarrow()
        except Exception:
            existing = dt.to_pyarrow_table().schema

        existing_map = {f.name: f.type for f in existing}
        incoming_names = {f.name for f in tbl.schema}

        arrays, fields = [], []

        for f in tbl.schema:
            name, src_type = f.name, f.type
            col = tbl.column(name)
            if name in existing_map and existing_map[name] != src_type:
                target = existing_map[name]
                try:
                    col = pc.cast(col, target, safe=False)
                except Exception:
                    col = pc.cast(col, pa.string(), safe=False)
                    target = pa.string()
                fields.append(pa.field(name, target))
                arrays.append(col)
            else:
                fields.append(pa.field(name, src_type))
                arrays.append(col)

        missing = [n for n in existing_map if n not in incoming_names]
        if missing:
            n_rows = tbl.num_rows
            for name in missing:
                t = existing_map[name]
                try:
                    arrays.append(pa.array([None] * n_rows, type=t))
                    fields.append(pa.field(name, t))
                except Exception:
                    arrays.append(pa.array([None] * n_rows, type=pa.string()))
                    fields.append(pa.field(name, pa.string()))

        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _verify_written_rows(
        self, s3_uri: str, sample_rows: list[dict[str, Any]], storage_options: dict
    ) -> None:
        """
        Read-after-write verification: ensure a sample of just-written keys is visible.

        Uses Delta -> PyArrow Dataset and a filter on externalId. This avoids scanning
        the whole table and works on non-partition columns.

        Raises RuntimeError if any sampled key is missing.
        """
        if not sample_rows:
            return
        external_ids_set: set[str] = {
            str(r["externalId"]) for r in sample_rows if r.get("externalId") is not None
        }
        external_ids: list[str] = sorted(external_ids_set)
        if not external_ids:
            return

        dt = DeltaTable(s3_uri, storage_options=storage_options)
        dset = dt.to_pyarrow_dataset()
        cond = ds.field("externalId").isin(external_ids)
        tbl = dset.to_table(
            filter=cond, columns=["space", "externalId", "lastUpdatedTime"]
        )

        have = {
            (
                tbl["space"][i].as_py(),
                tbl["externalId"][i].as_py(),
                tbl["lastUpdatedTime"][i].as_py(),
            )
            for i in range(tbl.num_rows)
        }
        want = {
            (r.get("space"), r.get("externalId"), r.get("lastUpdatedTime"))
            for r in sample_rows
        }
        missing = want - have
        if missing:
            raise RuntimeError(
                f"Post-write verification failed: {len(missing)} sampled rows not visible in {s3_uri}"
            )

    def _delta_append(self, table: str, rows: list[dict[str, Any]], space: str) -> None:
        """
        Safely append rows to S3-based Delta table with data validation and cleanup.
        This method writes CDF instance data to Delta Lake tables stored in S3, with built-in
        safety checks to prevent corruption and handle deletions (tombstone processing).
        Uses explicit data model version in the S3 path structure.
        """
        if not rows:
            return

        rows = self._sanitize_rows_for_tableau(rows)

        if not rows:
            self.logger.info(f"No data remaining after sanitization for {table}")
            return

        model = self._model_xid or "default"
        version = self._model_version
        if not version:
            raise RuntimeError(
                f"No data model version set for model {model} in space {space}"
            )

        assert self.s3_cfg is not None
        prefix = (self.s3_cfg.prefix.rstrip("/") + "/") if self.s3_cfg.prefix else ""
        s3_uri = f"s3://{self.s3_cfg.bucket}/{prefix}raw/{space}/{model}/{version}/views/{table}"

        storage_options = self._s3_storage_options()

        try:
            arrow_table = pa.Table.from_pylist(rows)
            arrow_table = self._coerce_nulltype_fields_to_string(arrow_table)
            arrow_table = self._align_to_existing_schema(s3_uri, arrow_table)
            self._validate_tableau_schema(arrow_table.schema, table)
        except Exception as schema_error:
            self.logger.warning(
                f"Standard schema failed for {table}: {schema_error}. Doing safe types fallback."
            )
            arrow_table = self._convert_to_safe_types(rows)

        if arrow_table is None:
            raise RuntimeError(f"All schema approaches failed for {table}")

        try:
            write_deltalake(
                s3_uri,
                arrow_table,
                mode="append",
                schema_mode="merge",
                storage_options=storage_options,
            )
            sample_rows = random.sample(rows, min(len(rows), 50))
            self._verify_written_rows(s3_uri, sample_rows, storage_options)
            tombstones = [r["externalId"] for r in rows if r.get("deletedTime")]
            if tombstones:
                dt = DeltaTable(
                    s3_uri,
                    storage_options=storage_options,
                )

                self._delete_tombstones(dt, tombstones)
        except DeltaError as err:
            self.logger.error("Delta write failed: %s", err)
            raise

    def _reload_remote_config(self) -> bool:
        """Reload config from remote source if it's a remote config."""

        if not (hasattr(self.config, "cognite") and self.config.cognite):
            self.logger.info("Not a remote config, skipping reload")
            return False

        try:
            extraction_pipeline = self.config.cognite.get_extraction_pipeline(
                self.cognite_client
            )
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
                extraction_pipeline_external_id=extraction_pipeline.external_id,
            )
            self.config = new_config
            self.s3_cfg = (
                self.config.destination.s3 if self.config.destination else None
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to reload remote config: {e}")
            return False
