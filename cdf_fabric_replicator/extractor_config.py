import logging
from pathlib import Path

from cognite.extractorutils import Extractor
from cognite.extractorutils.base import CancellationToken
from typing import Optional
from cognite.client.data_classes import ExtractionPipelineConfigWrite
from cognite.extractorutils.metrics import safe_get
import yaml

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics



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
        self.metrics = metrics
        self.stop_event = stop_event
       # self.override_config_path = override_config_path or Path(override_config_path).parent / override_config_path
        self.config_file_path = override_config_path or Path(override_config_path).parent / override_config_path
        self.external_id = None
        self.logger = logging.getLogger(self.name)
        self.logger.debug(f"Initialized CdfExtractorConfig with config_file_path: {self.config_file_path}")


    def run(self) -> None:
        self.logger.debug("Starting run method.")
        self.logger.debug("Cognite client initialized.")
        if self.config.extractor_pipeline is None:
            self.logger.info("No extractor pipeline spaces found in config")
            return
        self.external_id = self.config.extractor_pipeline.external_id
        self.logger.debug(f"Set external_id: {self.external_id}")
        self.logger.debug(f"Config file path: {self.config_file_path}")
        config_yaml_as_str = self.read_yaml_as_string(self.config_file_path)
        self.logger.debug(f"Read YAML config as string.{type(config_yaml_as_str)}")
        self.write_extraction_pipeline_config(self, config_yaml_as_str)
        self.logger.debug("Extraction pipeline config written.")


    def read_yaml_as_string(self, yaml_path: str):
        self.logger.debug(f"Reading YAML file from: {yaml_path}")
        #yaml_path = Path(yaml_path).parent / yaml_path

        try:
            with open(yaml_path, 'r') as file:
                data = yaml.safe_load(file)
            self.logger.debug("YAML file loaded successfully.")
            yaml_data = yaml.dump(data)
        except FileNotFoundError:
            self.logger.error(f"Config YAML file not found: {yaml_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading YAML file: {e}")
            raise
        return yaml_data



    def write_extraction_pipeline_config(self, config_yaml_as_str, str1):
        self.logger.debug("Writing extraction pipeline config to Cognite Data Fusion.")
        self.logger.debug(f"Config File as String {type(str1)} value: {str1}")
        try:
            self.cognite_client.extraction_pipelines.config.create(ExtractionPipelineConfigWrite(self.external_id, config=str1))
            self.logger.debug("Extraction pipeline config created successfully.")
        except Exception as exception:
            self.logger.error(f"Failed to write extraction pipeline config: {exception}")
            raise
        return

''' Local testing code given below, uncomment to run locally
print("CdfExtractorConfig initialized successfully.")
with CdfExtractorConfig(metrics=safe_get(Metrics), override_config_path="config_examples/example_config.yaml") as extractor_config:
    extractor_config.run()

'''