import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timedelta, UTC

from cdf_s3_replicator.data_modeling import DataModelingReplicator


@pytest.fixture
def test_replicator():
    """Create a bare replicator instance with minimal wiring."""
    with patch('cdf_s3_replicator.data_modeling.DataModelingReplicator.__init__', lambda self, *a, **k: None):
        r = DataModelingReplicator.__new__(DataModelingReplicator)
        r.logger = Mock()
        r._s3 = None
        r._s3_created_at = None
        r.s3_cfg = Mock()
        r.s3_cfg.bucket = 'test-bucket'
        r.s3_cfg.prefix = 'test-prefix'
        r.s3_cfg.region = 'us-east-1'
        r._model_xid = 'm'
        r._model_version = '1'
        return r


class TestS3Operations:

    def test_ensure_s3_creates_and_renews(self, test_replicator):
        """_ensure_s3 uses _make_s3_client and renews after ~50 minutes."""
        c1, c2 = object(), object()

        calls = {"n": 0}

        def fake_make(_self=None):
            calls["n"] += 1
            return c1 if calls["n"] == 1 else c2

        with patch.object(DataModelingReplicator, "_make_s3_client", side_effect=fake_make):
            test_replicator._ensure_s3()
            assert test_replicator._s3 is c1
            assert isinstance(test_replicator._s3_created_at, datetime)

            t_initial = test_replicator._s3_created_at
            test_replicator._ensure_s3()
            assert test_replicator._s3 is c1
            assert test_replicator._s3_created_at == t_initial

            test_replicator._s3_created_at = datetime.now(UTC) - timedelta(minutes=51)
            test_replicator._ensure_s3()
            assert test_replicator._s3 is c2
            assert test_replicator._s3_created_at.tzinfo is UTC

    def test_atomic_replace_files_success(self, test_replicator):
        """Successful atomic replacement calls copy_and_verify then deletes temps."""
        s3 = Mock()
        test_replicator._s3 = s3
        test_replicator._s3_created_at = datetime.now(UTC)
        updates = [
            ('s3://test-bucket/tmp_file.parquet', 's3://test-bucket/final_file.parquet', 'nodes')
        ]

        with patch.object(DataModelingReplicator, "_copy_and_verify", return_value=None) as mock_copy:
            test_replicator._atomic_replace_files(updates)

        mock_copy.assert_called_once_with(
            "test-bucket", "tmp_file.parquet", "test-bucket", "final_file.parquet"
        )
        s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="tmp_file.parquet")

    def test_atomic_replace_files_cleanup_on_failure(self, test_replicator):
        """If copy fails, temps are still cleaned up."""
        s3 = Mock()
        test_replicator._s3 = s3
        test_replicator._s3_created_at = datetime.now(UTC)

        updates = [
            ('s3://test-bucket/tmp1.parquet', 's3://test-bucket/final1.parquet', 'edges'),
            ('s3://test-bucket/tmp2.parquet', 's3://test-bucket/final2.parquet', 'nodes'),
        ]

        with patch.object(DataModelingReplicator, "_copy_and_verify", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                test_replicator._atomic_replace_files(updates)

        deleted = {(kw["Bucket"], kw["Key"]) for _, kw in s3.delete_object.call_args_list}
        assert deleted == {("test-bucket", "tmp1.parquet"), ("test-bucket", "tmp2.parquet")}

    def test_cleanup_temp_files_mock(self, test_replicator):
        """
        _cleanup_temp_files lists objects under the publish dir and deletes
        any that contain the provided temp suffix.
        """
        s3 = Mock()
        test_replicator._s3 = s3
        test_replicator._s3_created_at = datetime.now(UTC)

        pub_dir = "s3://test-bucket/test-prefix/publish/test_space/viewA/"
        suffix = "_temp_1234"
        s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "test-prefix/publish/test_space/viewA/nodes_temp_1234.parquet"},
                {"Key": "test-prefix/publish/test_space/viewA/edges_temp_1234.parquet"},
                {"Key": "test-prefix/publish/test_space/viewA/edges.parquet"},
            ]
        }

        test_replicator._cleanup_temp_files(pub_dir, suffix)

        s3.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="test-prefix/publish/test_space/viewA/"
        )
        s3.delete_objects.assert_called_once()
        args, kwargs = s3.delete_objects.call_args
        objs = kwargs["Delete"]["Objects"]
        keys = {o["Key"] for o in objs}
        assert keys == {
            "test-prefix/publish/test_space/viewA/nodes_temp_1234.parquet",
            "test-prefix/publish/test_space/viewA/edges_temp_1234.parquet",
        }

    def test_raw_prefix(self, test_replicator):
        """_raw_prefix uses model/version and optional prefix cleanly."""
        got = test_replicator._raw_prefix("test_space")
        assert got == "s3://test-bucket/test-prefix/raw/test_space/m/1"
        assert "//raw/" not in got

    def test_publish_prefix(self, test_replicator):
        got = test_replicator._publish_prefix("test_space")
        assert got == "s3://test-bucket/test-prefix/publish/test_space/m/1"
