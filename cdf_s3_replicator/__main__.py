import logging
import threading

from cognite.extractorutils.base import CancellationToken

from cognite.extractorutils.metrics import safe_get

from cdf_s3_replicator.data_modeling import DataModelingReplicator

from cdf_s3_replicator.metrics import Metrics
from cdf_s3_replicator.log_config import LOGGING_CONFIG


def main() -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logging.info("Starting CDF S3 Replicator")
    stop_event = CancellationToken()
    worker_list = []

    with DataModelingReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as dm_replicator:
        worker_list.append(threading.Thread(target=dm_replicator.run))

    for worker in worker_list:
        worker.start()

    for worker in worker_list:
        worker.join()


if __name__ == "__main__":
    main()
