import os
import pathlib
import yaml
import pytest
from dotenv import load_dotenv

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials

load_dotenv()

_DEFAULT_TEST_CONFIG = (pathlib.Path(__file__).parent / "test_config.yaml").resolve()
os.environ.setdefault("TEST_CONFIG_PATH", str(_DEFAULT_TEST_CONFIG))

REQUIRED_ENV = [
    "COGNITE_BASE_URL",
    "COGNITE_PROJECT",
    "COGNITE_TOKEN_URL",
    "COGNITE_CLIENT_ID",
    "COGNITE_CLIENT_SECRET",
    "COGNITE_TOKEN_SCOPES",
    "COGNITE_STATE_DB",
    "COGNITE_STATE_TABLE",
    "COGNITE_EXTRACTION_PIPELINE",
    "AWS_REGION",
    "AWS_S3_BUCKET",
]

_missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if not (os.getenv("S3_PREFIX") or os.getenv("AWS_S3_BUCKET")):
    _missing.append("S3_PREFIX or AWS_S3_BUCKET")

if _missing:
    pytest.skip(
        f"Skipping integration tests: missing env vars: {', '.join(_missing)}",
        allow_module_level=True,
    )

os.environ.setdefault("COGNITE_CLIENT_NAME", "cdf-s3-replicator-tests")


@pytest.fixture(scope="session")
def test_config():
    cfg_path = os.environ["TEST_CONFIG_PATH"]
    p = pathlib.Path(cfg_path)
    if not p.is_file():
        pytest.skip(
            f"TEST_CONFIG_PATH does not point to a file: {cfg_path}",
            allow_module_level=True,
        )
    with p.open() as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    credentials = OAuthClientCredentials(
        token_url=os.environ["COGNITE_TOKEN_URL"],
        client_id=os.environ["COGNITE_CLIENT_ID"],
        client_secret=os.environ["COGNITE_CLIENT_SECRET"],
        scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
    )
    return CogniteClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=credentials,
        )
    )
