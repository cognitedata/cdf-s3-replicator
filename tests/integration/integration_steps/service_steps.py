from cdf_s3_replicator.time_series import TimeSeriesReplicator
from cdf_s3_replicator.extractor import CdfS3Extractor
from pandas import DataFrame


def run_replicator(test_replicator: TimeSeriesReplicator):
    # Processes data point subscription batches
    test_replicator.process_subscriptions()


def run_extractor(test_extractor: CdfS3Extractor, data_frame: DataFrame):
    # Processes data point subscription batches
    test_extractor.write_time_series_to_cdf(data_frame)


def start_replicator():
    # Start the replicator service
    pass


def stop_replicator():
    # Stop the replicator service
    pass
