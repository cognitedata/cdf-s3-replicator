import logging
from pathlib import Path
from cognite.client.data_classes.data_modeling import CDFExternalIdReference
from cognite.extractorutils import Extractor
from cognite.extractorutils.threading import CancellationToken
from typing import Optional
from cognite.client.data_classes import ExtractionPipelineConfigWrite
from dotenv import load_dotenv
import yaml
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.extractorutils.configtools.loaders import load_yaml
import os
import re


class EnvironmentVariableResolver:
    """Generic environment variable resolver that works with any variable names"""
    
    @staticmethod
    def extract_variable_names(text):
        """
        Extract all variable names from ${VAR_NAME} patterns in a string.
        Returns a list of variable names found.
        """
        if not isinstance(text, str):
            return []
        
        # Find all ${VARIABLE_NAME} patterns
        pattern = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')
        matches = pattern.findall(text)
        return matches
    
    @staticmethod
    def check_env_variables_exist(variable_names):
        """
        Check which variables from the list exist in environment variables.
        Returns a dict with variable_name -> (exists, value)
        """
        load_dotenv()  # Ensure .env file is loaded
        
        result = {}
        for var_name in variable_names:
            value = os.environ.get(var_name)
            result[var_name] = {
                'exists': value is not None,
                'value': value if value is not None else f"${{{var_name}}}"  # Keep original if not found
            }
        return result
    
    @staticmethod
    def resolve_string(text):
        """
        Resolve environment variables in a string.
        Only replaces variables that actually exist in the environment.
        """
        if not isinstance(text, str):
            return text
        
        # Extract all variable names from the string
        variable_names = EnvironmentVariableResolver.extract_variable_names(text)
        
        if not variable_names:
            return text  # No variables to resolve
        
        # Check which variables exist in environment
        env_check = EnvironmentVariableResolver.check_env_variables_exist(variable_names)
        
        # Replace only existing variables
        result = text
        for var_name, info in env_check.items():
            if info['exists']:
                pattern = f"${{{var_name}}}"
                result = result.replace(pattern, info['value'])
        
        return result
    
    @staticmethod
    def resolve_yaml_structure(obj, path=""):
        """
        Recursively resolve environment variables in a nested YAML structure.
        """
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                current_path = f"{path}.{k}" if path else k
                result[k] = EnvironmentVariableResolver.resolve_yaml_structure(v, current_path)
            return result
        elif isinstance(obj, list):
            return [
                EnvironmentVariableResolver.resolve_yaml_structure(item, f"{path}[{idx}]") 
                for idx, item in enumerate(obj)
            ]
        elif isinstance(obj, str):
            return EnvironmentVariableResolver.resolve_string(obj)
        else:
            return obj


class CdfExtractorConfig(Extractor[Config]):
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken | None = None,
        name: str = "extractor_pipeline_config_to_cdf",
        override_config_path: Optional[str] = None,
    ) -> None:
        # Load environment variables first
        load_dotenv()
        
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
        """
        try:

            client_external = config.cognite.get_cognite_client(name)
            config_data = client_external.extraction_pipelines.config.retrieve(
                external_id=extraction_pipeline_external_id
            )
            # Parse the YAML string and format it properly
            parsed_config = yaml.safe_load(config_data.config)
            formatted_config = yaml.dump(parsed_config, default_flow_style=False, indent=2, sort_keys=False)
            logging.getLogger("retrieve_pipeline_config_standalone").info(
                f"Config data retrieved:\n{formatted_config}"
            )
            # config_data.config is the YAML string
            new_config = load_yaml(
                source=config_data.config,
                config_type=type(config)
            )
            logging.getLogger("extraction_pipeline_config_standalone").info(f"{new_config.destination.s3.prefix}|||{new_config.destination.s3.bucket}|||{new_config.destination.s3.region}")
            return new_config
        except Exception as e:
            logging.getLogger("retrieve_pipeline_config_standalone").error(
                f"Failed to retrieve pipeline config: {e}"
            )
            raise

    @staticmethod
    def resolve_env_vars_in_yaml(yaml_source):
        """
        Generic environment variable resolution for YAML files.
        Resolves any ${VARIABLE_NAME} pattern where VARIABLE_NAME exists in environment.
        """
        load_dotenv()  # Ensure .env file is loaded
        
        # Load YAML from file or string
        if isinstance(yaml_source, str) and os.path.exists(yaml_source):
            with open(yaml_source, 'r') as f:
                data = yaml.safe_load(f.read())
        else:
            data = yaml.safe_load(yaml_source)
        
        # Resolve environment variables
        resolved_data = EnvironmentVariableResolver.resolve_yaml_structure(data)
        
        return resolved_data

    @staticmethod
    def load_yaml_with_env(yaml_source):
        """
        Load a YAML config file or string and resolve environment variables.
        This is a wrapper around resolve_env_vars_in_yaml for compatibility.
        """
        return CdfExtractorConfig.resolve_env_vars_in_yaml(yaml_source)

    def write_extraction_pipeline_config(self, config_yaml_as_str=None):
        """
        Write the extraction pipeline config to Cognite Data Fusion.
        Resolves environment variables before writing.
        """
        self.logger.debug("Writing extraction pipeline config to Cognite Data Fusion.")
        
        if config_yaml_as_str is None:
            # Use the resolved config dict
            resolved_config = self.get_resolved_config_dict()
            config_yaml_as_str = yaml.dump(resolved_config)
        else:
            # Resolve environment variables in the provided YAML string
            self.logger.info("Resolving environment variables in provided YAML string")
            resolved_data = self.resolve_env_vars_in_yaml(config_yaml_as_str)
            config_yaml_as_str = yaml.dump(resolved_data)
        
        if not self.config.extractor_pipeline or not getattr(self.config.extractor_pipeline, 'external_id', None):
            raise ValueError("extractor_pipeline or its external_id is not set in config.")
        
        self.logger.info(f"Final config YAML to be written:\n{config_yaml_as_str}")
        
        external_id = self.config.extractor_pipeline.external_id
        pipeline_list = self.client.extraction_pipelines.retrieve(external_id=external_id)
        
        try:
            if pipeline_list is not None:
                response = self.client.extraction_pipelines.config.create(
                    ExtractionPipelineConfigWrite(external_id=external_id, config=config_yaml_as_str))
                self.logger.debug(f"Extraction pipeline config created successfully: {str(response)}")
            else:
                self.logger.error("Extraction Pipeline not found as per the configured External ID.")
        except Exception as exception:
            self.logger.error(f"Failed to write extraction pipeline config: {exception}")
            raise
        return

    def retrieve_pipeline_config(self):
        config_data = self.client.extraction_pipelines.config.retrieve(external_id=self.config.extractor_pipeline.external_id)
        config_obj = yaml.safe_load(config_data.config)
        self.logger.info(f"config_obj retrieved from pipeline config: {self.config.extractor_pipeline.external_id} \n {yaml.dump(config_obj)}")

        return config_obj

    def get_resolved_config_dict(self):
        """
        Return the config as a dictionary with all environment variables interpolated.
        """
        load_dotenv()  # Ensure env vars are loaded
        
        import dataclasses
        from typing import Any
        
        def resolve_env_vars(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: resolve_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [resolve_env_vars(i) for i in obj]
            elif isinstance(obj, str):
                return EnvironmentVariableResolver.resolve_string(obj)
            elif dataclasses.is_dataclass(obj):
                return resolve_env_vars(dataclasses.asdict(obj))
            else:
                return obj
        
        # Convert config to dict if it's a dataclass
        if dataclasses.is_dataclass(self.config):
            config_dict = dataclasses.asdict(self.config)
        else:
            config_dict = self.config.__dict__ if hasattr(self.config, '__dict__') else self.config
        
        return resolve_env_vars(config_dict)


def test_env_vars():
    """Test if environment variables are accessible"""
    load_dotenv()
    print("Environment variables loaded from .env:")
    for key, value in os.environ.items():
        if 'COGNITE' in key or 'AWS' in key:
            print(f"  {key} = {value}")


# test
'''print(f"started Extractor config")
with CdfExtractorConfig(
    metrics=Metrics(),
    stop_event=CancellationToken(),
    name="extractor_config",
    override_config_path="config_examples/example_config.yaml"
) as extractor_config:
    extractor_config.run()
    # Retrieve the config as a YAML string
    with open("config_examples/example_config.yaml", "r") as f:
        config_yaml = f.read()

    # Write the extraction pipeline config using the YAML string
    extractor_config.write_extraction_pipeline_config(config_yaml)
    extractor_config.retrieve_pipeline_config()
'''
#test_env_vars()