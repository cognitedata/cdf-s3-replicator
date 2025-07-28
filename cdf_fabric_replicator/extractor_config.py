import logging
from pathlib import Path
from cognite.extractorutils import Extractor
from cognite.extractorutils.base import CancellationToken
from typing import Optional
import yaml
from cognite.extractorutils.metrics import safe_get

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.config import DataModelingConfig, DMModel


class CdfExtractorConfig(Extractor[Config]):
    def __init__(
            self,
            metrics: Metrics,
            stop_event: CancellationToken | None = None,
            name: str = "extractor_pipeline_config_to_cdf",
            override_config_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            name=name,
            description="Config File -> CDF",
            config_class=Config,
            metrics=metrics,
            version=__version__,
            config_file_path=override_config_path
        )
        try:
            self.dataset_name = None
            self.dataset_external_id = None
            self.client = None
            self.metrics = metrics
            self.stop_event = stop_event
            if override_config_path:
                self.config_file_path = Path(override_config_path)
            self.external_id = None
            self.logger = logging.getLogger(self.name)
        except Exception as e:
            self.logger.error(f"Error initializing CdfExtractorConfig: {e}")
            raise

    def run(self) -> None:
        self.client = self.config.cognite.get_cognite_client(self.name)
        if self.config.extractor_pipeline is None:
            self.logger.info("No extractor pipeline spaces found in config")
            return
        self.logger.info(f"Extractor pipeline found in config.{self.config.extractor_pipeline.dataset_external_id}")
        self.dataset_external_id = self.config.extractor_pipeline.dataset_external_id
        self.dataset_name = self.config.extractor_pipeline.dataset_external_id

    def read_yaml_as_string(self, yaml_path: Path):
        try:
            with open(yaml_path, 'r') as file:
                data = yaml.safe_load(file)
        except FileNotFoundError:
            self.logger.error(f"Config YAML file not found: {yaml_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading YAML file: {e}")
            raise
        return yaml.dump(data)

    @classmethod
    def retrieve_pipeline_config(cls, config, name: str, extraction_pipeline_external_id: str):
        try:
            client_external = config.cognite.get_cognite_client(name)

            config_response = client_external.extraction_pipelines.config.retrieve(
                external_id=extraction_pipeline_external_id
            )

            if config_response and config_response.config:
                config_string = config_response.config
                config_dict_ext = yaml.safe_load(config_string)

                if config_dict_ext:
                    if 'data-modeling' in config_dict_ext:
                        dm_configs = []
                        for dm_dict in config_dict_ext['data-modeling']:
                            dm_models = []
                            if 'data_models' in dm_dict and dm_dict['data_models']:
                                for model_dict in dm_dict['data_models']:
                                    dm_model = DMModel(
                                        external_id=model_dict['external_id'],
                                        views=model_dict.get('views'),
                                        version=model_dict.get('version')
                                    )
                                    dm_models.append(dm_model)

                            dm_config = DataModelingConfig(
                                space=dm_dict['space'],
                                views=dm_dict.get('views'),
                                data_models=dm_models if dm_models else None
                            )
                            dm_configs.append(dm_config)

                        config.data_modeling = dm_configs

                    if 'extractor' in config_dict_ext:
                        extractor_dict = config_dict_ext['extractor']
                        if 'poll_time' in extractor_dict:
                            config.extractor.poll_time = extractor_dict['poll_time']
                        if 'snapshot_interval' in extractor_dict:
                            config.extractor.snapshot_interval = extractor_dict['snapshot_interval']

                    if 'destination' in config_dict_ext and config_dict_ext['destination']:
                        dest_dict = config_dict_ext['destination']
                        if 's3' in dest_dict and dest_dict['s3']:
                            s3_dict = dest_dict['s3']
                            if config.destination and config.destination.s3:
                                if 'bucket' in s3_dict:
                                    config.destination.s3.bucket = s3_dict['bucket']
                                if 'prefix' in s3_dict:
                                    config.destination.s3.prefix = s3_dict['prefix']
                                if 'region' in s3_dict:
                                    config.destination.s3.region = s3_dict['region']

                return config
            else:
                logging.getLogger("retrieve_pipeline_config").warning(
                    "No config found or config is empty"
                )
                return config

        except Exception as e:
            logging.getLogger("retrieve_pipeline_config").error(
                f"Failed to retrieve pipeline config: {e}"
            )
            raise