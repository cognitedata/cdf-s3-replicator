import pytest
import os
from unittest.mock import patch, Mock

from cdf_s3_replicator.data_modeling import DataModelingReplicator


@pytest.fixture
def mock_env_vars():
    """Mock AWS environment variables"""
    with patch.dict(os.environ, {
        'AWS_ACCESS_KEY_ID': 'test-access-key',
        'AWS_SECRET_ACCESS_KEY': 'test-secret-key',
        'AWS_REGION': 'us-east-1'
    }):
        yield


@pytest.fixture
def test_replicator():
    """Create a test replicator instance"""
    with patch('cdf_s3_replicator.data_modeling.DataModelingReplicator.__init__',
               lambda x, *args, **kwargs: None):
        replicator = DataModelingReplicator.__new__(DataModelingReplicator)
        replicator.logger = Mock()
        replicator._s3 = None
        replicator.s3_cfg = Mock()
        replicator.s3_cfg.bucket = 'test-bucket'
        replicator.s3_cfg.prefix = 'test-prefix'
        replicator.s3_cfg.region = 'us-east-1'
        replicator.LARGE_TABLE_THRESHOLD = 1_000_000
        return replicator


class TestS3OperationsSimple:

    def test_ensure_s3_success(self, test_replicator, mock_env_vars):
        """Test successful S3 client initialization"""
        with patch('boto3.client') as mock_boto3_client:
            mock_client = Mock()
            mock_boto3_client.return_value = mock_client

            test_replicator._ensure_s3()

            assert test_replicator._s3 == mock_client
            mock_boto3_client.assert_called_once_with(
                "s3",
                aws_access_key_id="test-access-key",
                aws_secret_access_key="test-secret-key",
                region_name="us-east-1"
            )

    def test_ensure_s3_missing_credentials(self, test_replicator):
        """Test S3 client initialization with missing credentials"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError) as exc_info:
                test_replicator._ensure_s3()

            assert "Missing required environment variables" in str(exc_info.value)
            assert "AWS_ACCESS_KEY_ID" in str(exc_info.value)
            assert "AWS_SECRET_ACCESS_KEY" in str(exc_info.value)
            assert "AWS_REGION" in str(exc_info.value)

    def test_ensure_s3_partial_credentials(self, test_replicator):
        """Test S3 client initialization with partial credentials"""
        with patch.dict(os.environ, {'AWS_ACCESS_KEY_ID': 'test-key'}, clear=True):
            with pytest.raises(RuntimeError) as exc_info:
                test_replicator._ensure_s3()

            assert "Missing required environment variables" in str(exc_info.value)
            assert "AWS_SECRET_ACCESS_KEY" in str(exc_info.value)
            assert "AWS_REGION" in str(exc_info.value)
            assert "AWS_ACCESS_KEY_ID" not in str(exc_info.value)

    def test_atomic_replace_files_success(self, test_replicator, mock_env_vars):
        """Test successful atomic file replacement with mocked S3"""
        mock_s3_client = Mock()
        test_replicator._s3 = mock_s3_client

        file_updates = [
            ('s3://test-bucket/temp_file.parquet', 's3://test-bucket/final_file.parquet', 'test')
        ]

        test_replicator._atomic_replace_files(file_updates)

        mock_s3_client.copy_object.assert_called_once()

        mock_s3_client.delete_object.assert_called_once()

    def test_cleanup_temp_files_mock(self, test_replicator, mock_env_vars):
        """Test cleanup of temporary files with mocked S3"""
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'path/file_temp_123.parquet'},
                {'Key': 'path/other_file.parquet'}
            ]
        }
        test_replicator._s3 = mock_s3_client

        test_replicator._cleanup_temp_files('s3://test-bucket/path/', '_temp_123')

        mock_s3_client.list_objects_v2.assert_called_once()
        mock_s3_client.delete_objects.assert_called_once()

    def test_raw_prefix(self, test_replicator):
        """Test raw prefix generation"""
        result = test_replicator._raw_prefix('test_space')
        expected = 's3://test-bucket/test-prefix/raw/test_space'
        assert result == expected

    def test_publish_prefix(self, test_replicator):
        """Test publish prefix generation"""
        result = test_replicator._publish_prefix('test_space')
        expected = 's3://test-bucket/test-prefix/publish/test_space'
        assert result == expected