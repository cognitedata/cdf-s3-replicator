import logging
from pathlib import Path
from cognite.client.data_classes.data_modeling import CDFExternalIdReference
from cognite.extractorutils import Extractor
from cognite.extractorutils.threading import CancellationToken
from typing import Optional
from cognite.client.data_classes import ExtractionPipelineConfigWrite
import yaml
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.extractorutils.configtools.loaders import load_yaml


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
            self.external_id = None
            self.logger = logging.getLogger(self.name)
            self.logger.debug(f"Initialized CdfExtractorConfig with config_file_path: {self.config_file_path}")
        except Exception as e:
            self.logger.error(f"Error initializing CdfExtractorConfig: {e}")
            raise

    def run(self) -> None:
        self.logger.debug("Starting run method.")
        self.client = self.config.cognite.get_cognite_client(self.name)
        self.logger.debug("Cognite client initialized.")
        self.logger.debug(f"Config keys: {list(self.config.__dict__.keys())}")
        if self.config.extractor_pipeline is None:
            self.logger.info("No extractor pipeline spaces found in config")
            return
        self.logger.debug(f"Extractor pipeline found in config.{self.config.extractor_pipeline.dataset_external_id}")
        self.dataset_external_id = self.config.extractor_pipeline.dataset_external_id
        self.dataset_name = self.config.extractor_pipeline.dataset_external_id
        self.logger.debug("Extraction pipeline config written.")

    def read_yaml_as_string(self, yaml_path: Path):
        self.logger.debug(f"Reading YAML file from: {yaml_path}")
        try:
            with open(yaml_path, 'r') as file:
                data = yaml.safe_load(file)
            self.logger.debug("YAML file loaded successfully.")
        except FileNotFoundError:
            self.logger.error(f"Config YAML file not found: {yaml_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading YAML file: {e}")
            raise
        return yaml.dump(data)

    @staticmethod
    def retrieve_pipeline_config_standalone(config, name: str, extraction_pipeline_external_id: str):
        """
        Retrieve the extraction pipeline config from Cognite Data Fusion using config object.

        Args:
            config: Config object with get_cognite_client method.
            name: Name for the Cognite client.
            extraction_pipeline_external_id: External ID of the extraction pipeline.

        Returns:
            The extraction pipeline config as a Config object.
        """
        try:
            client_external = config.cognite.get_cognite_client(name)
            config_data = client_external.extraction_pipelines.config.retrieve(
                external_id=extraction_pipeline_external_id
            )
            logging.getLogger("retrieve_pipeline_config_standalone").debug(
                f"Config data retrieved: {yaml.dump(config_data.config)}"
            )
            # config_data.config is the YAML string
            new_config = load_yaml(
                source=config_data.config,
                config_type=type(config)
            )
            return new_config
        except Exception as e:
            logging.getLogger("retrieve_pipeline_config_standalone").error(
                f"Failed to retrieve pipeline config: {e}"
            )
            raise



    def write_extraction_pipeline_config(self, config_yaml_as_str):
        self.logger.debug("Writing extraction pipeline config to Cognite Data Fusion.")
        pipeline_list = self.client.extraction_pipelines.retrieve(external_id=self.extraction_pipeline.external_id)
        try:
            if pipeline_list is not None:
                response = self.client.extraction_pipelines.config.create(
                ExtractionPipelineConfigWrite(external_id=self.extraction_pipeline.external_id, config=config_yaml_as_str))
                self.logger.debug(f"Extraction pipeline config created successfully: {str(response)}")
            else:
                self.logger.error("Extraction Pipeline not found as per the configured External ID. Please check whether it is available.")
        except Exception as exception:
            self.logger.error(f"Failed to write extraction pipeline config: {exception}")
            raise
        return

    def retrieve_pipeline_config(self):
        config_data = self.client.extraction_pipelines.config.retrieve(external_id=self.extraction_pipeline.external_id )
        config_obj = yaml.safe_load(config_data.config)
        self.logger.debug(f"config_obj retrieved: {yaml.dump(config_obj)}")
        return config_obj


# test
print(f"started Extractor config")
'''with CdfExtractorConfig(
    metrics=Metrics(),
    stop_event=CancellationToken(),
    name="extractor_config",
    override_config_path="/Users/dev/replicator/config_examples/example_config.yaml"
) as extractor_config:
    extractor_config.run()
    # Retrieve the config as a YAML string
    with open("/Users/dev/replicator/config_examples/example_config.yaml", "r") as f:
        config_yaml = yaml.safe_load(f)

    # Write the extraction pipeline config using the YAML string
    config_yaml_str = yaml.dump(config_yaml)
    extractor_config.write_extraction_pipeline_config(config_yaml_str)
    extractor_config.retrieve_pipeline_config()'''