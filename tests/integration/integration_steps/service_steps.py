from __future__ import annotations

import threading
from typing import Optional

from cdf_s3_replicator.data_modeling import DataModelingReplicator


def run_data_model_replicator_once(replicator: DataModelingReplicator) -> None:
    """
    Run a single processing cycle for all configured spaces/models/views.
    This is basically a thin wrapper around `process_spaces()` which writes to RAW paths.
    """
    replicator.process_spaces()


def run_publish_snapshots_once(replicator: DataModelingReplicator) -> None:
    """
    Force a single snapshot publish pass for all configured spaces/models/views.
    Writes /publish/<space>/<model>/<version>/views/{<view>/nodes.parquet, <view>/edges.parquet}.
    """
    if not replicator.config or not replicator.config.data_modeling:
        return

    for dm_cfg in replicator.config.data_modeling:
        replicator._publish_space_snapshots(dm_cfg)


def start_data_model_replicator(replicator: DataModelingReplicator) -> threading.Thread:
    """
    Start the replicator's long-running loop in a background thread.
    You must stop it with `stop_data_model_replicator(...)`.
    """
    worker = threading.Thread(target=replicator.run, daemon=True)
    worker.start()
    return worker


def stop_data_model_replicator(replicator: DataModelingReplicator, worker: Optional[threading.Thread], timeout: float = 10.0) -> None:
    """
    Signal the background replicator to stop and join the thread.
    Safe to call even if `worker` is None.
    """
    try:
        replicator.stop_event.set()
    except Exception:
        pass

    if worker is not None:
        worker.join(timeout=timeout)
