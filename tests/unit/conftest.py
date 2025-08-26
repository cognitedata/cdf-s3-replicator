import pytest
from cognite.extractorutils.configtools import load_yaml
from cdf_s3_replicator.config import Config


@pytest.fixture(scope="session")
def config_raw():
    yield """
    type: local

    logger:
      console:
        level: INFO

    cognite:
      host: https://api.cognitedata.com
      project: unit_test_extractor

      idp-authentication:
        token-url: https://get-a-token.com/token
        client-id: abc123
        secret: def567
        scopes:
          - https://api.cognitedata.com/.default
      extraction-pipeline:
        external-id: test-s3-extractor

    extractor:
      state-store:
        local:
          path: test_states.json
      subscription-batch-size: 10000
      ingest-batch-size: 100000
      poll-time: 5
      snapshot-interval: 5

    # Keep a tiny DM section so anything that reads it won't blow up.
    data_modeling:
      - space: unit-space
        data_models:
          - external_id: unit-model
            views: [ViewA, ViewB]
            version: "1"

    destination:
      s3:
        bucket: test-bucket
        prefix: pre
        region: us-east-1
    """


@pytest.fixture(scope="session")
def config(config_raw):
    yield load_yaml(config_raw, Config)
