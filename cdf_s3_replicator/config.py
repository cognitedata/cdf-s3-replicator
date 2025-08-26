from dataclasses import dataclass
from typing import List, Optional

from cognite.extractorutils.configtools import BaseConfig, StateStoreConfig
from cognite.extractorutils.configtools import LoggingConfig


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig
    subscription_batch_size: int = 10_000
    ingest_batch_size: int = 100_000
    s3_ingest_batch_size: int = 1_000
    poll_time: int = 3600
    snapshot_interval: int = 900


@dataclass
class ExtractorPipelineConfig:
    external_id: str
    dataset_external_id: Optional[str] = None
    dataset_name: Optional[str] = None


@dataclass
class DMModel:
    external_id: str
    views: list[str] | None = None
    version: int | str | None = None


@dataclass
class DataModelingConfig:
    space: str
    views: list[str] | None = None
    data_models: list[DMModel] | None = None


@dataclass
class S3DestinationConfig:
    bucket: str
    prefix: Optional[str] = None
    region: Optional[str] = None


@dataclass
class DestinationConfig:
    s3: Optional[S3DestinationConfig] = None


@dataclass
class Config(BaseConfig):
    logger: LoggingConfig
    extractor: ExtractorConfig
    destination: Optional[DestinationConfig]
    data_modeling: Optional[List[DataModelingConfig]]
