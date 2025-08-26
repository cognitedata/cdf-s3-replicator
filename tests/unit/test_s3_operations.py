from types import SimpleNamespace

import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timedelta, timezone
from cdf_s3_replicator.data_modeling import DataModelingReplicator

UTC = timezone.utc


@pytest.fixture
def test_replicator():
    """Create a bare replicator instance with minimal wiring."""
    with patch(
        "cdf_s3_replicator.data_modeling.DataModelingReplicator.__init__",
        lambda self, *a, **k: None,
    ):
        r = DataModelingReplicator(metrics=Mock(), stop_event=Mock())
        r.logger = Mock()
        r.s3_cfg = SimpleNamespace(
            bucket="test-bucket", prefix="pre", region="us-east-1"
        )
        r.config = SimpleNamespace(destination=SimpleNamespace(s3=r.s3_cfg))
        r.state_store = Mock()
        r.cognite_client = Mock()
        r._s3 = None
        r._s3_created_at = None
        r._model_xid = "m"
        r._model_version = "1"
        r._expected_node_props = None
        r._view_props_by_xid = {}
        return r


class TestS3Operations:
    def test_ensure_s3_creates_and_renews(self, test_replicator):
        """_ensure_s3 uses _make_s3_client and renews after ~50 minutes."""
        c1, c2 = object(), object()

        calls = {"n": 0}

        def fake_make(_self=None):
            calls["n"] += 1
            return c1 if calls["n"] == 1 else c2

        test_replicator._s3 = None
        test_replicator._s3_created_at = None

        with patch.object(
            DataModelingReplicator, "_make_s3_client", side_effect=fake_make
        ):
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
            (
                "s3://test-bucket/tmp_file.parquet",
                "s3://test-bucket/final_file.parquet",
                "nodes",
            )
        ]

        with patch.object(
            DataModelingReplicator, "_copy_and_verify", return_value=None
        ) as mock_copy:
            test_replicator._atomic_replace_files(updates)

        mock_copy.assert_called_once_with(
            "test-bucket", "tmp_file.parquet", "test-bucket", "final_file.parquet"
        )
        s3.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="tmp_file.parquet"
        )

    def test_atomic_replace_files_cleanup_on_failure(self, test_replicator):
        """If copy fails, temps are still cleaned up."""
        s3 = Mock()
        test_replicator._s3 = s3
        test_replicator._s3_created_at = datetime.now(UTC)

        updates = [
            (
                "s3://test-bucket/tmp1.parquet",
                "s3://test-bucket/final1.parquet",
                "edges",
            ),
            (
                "s3://test-bucket/tmp2.parquet",
                "s3://test-bucket/final2.parquet",
                "nodes",
            ),
        ]

        with patch.object(
            DataModelingReplicator, "_copy_and_verify", side_effect=RuntimeError("boom")
        ):
            with pytest.raises(RuntimeError, match="boom"):
                test_replicator._atomic_replace_files(updates)

        deleted = {
            (kw["Bucket"], kw["Key"]) for _, kw in s3.delete_object.call_args_list
        }
        assert deleted == {
            ("test-bucket", "tmp1.parquet"),
            ("test-bucket", "tmp2.parquet"),
        }

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
        test_replicator._model_xid = "m"
        test_replicator._model_version = "1"

        got = test_replicator._raw_prefix("test_space")
        assert got == "s3://test-bucket/pre/raw/test_space/m/1"
        assert "//raw/" not in got

    def test_publish_prefix(self, test_replicator):
        test_replicator._model_xid = "m"
        test_replicator._model_version = "1"

        got = test_replicator._publish_prefix("test_space")
        assert got == "s3://test-bucket/pre/publish/test_space/m/1"

    def test_get_s3_prefix_requires_version(self, test_replicator):
        test_replicator._model_xid = "m"
        test_replicator._model_version = None
        with pytest.raises(RuntimeError):
            test_replicator._get_s3_prefix("space", "raw")

    def test_ensure_s3_recreates_client_after_50_minutes(
        self, monkeypatch, test_replicator
    ):
        first = object()
        second = object()

        calls = {"n": 0}

        def fake_make():
            calls["n"] += 1
            return first if calls["n"] == 1 else second

        monkeypatch.setattr(
            DataModelingReplicator, "_make_s3_client", lambda self: fake_make()
        )

        test_replicator._s3 = None
        test_replicator._s3_created_at = None
        test_replicator._ensure_s3()
        assert test_replicator._s3 is first

        test_replicator._s3_created_at = datetime(2020, 1, 1, tzinfo=UTC)
        test_replicator._ensure_s3()
        assert test_replicator._s3 is second

    @patch.object(DataModelingReplicator, "_list_prefixes")
    def test_edge_folders_for_anchor_filters_correctly(
        self, mock_list, test_replicator
    ):
        test_replicator._model_xid = "m"
        test_replicator._model_version = "1"
        test_replicator.s3_cfg = SimpleNamespace(bucket="test-bucket", prefix="pre")

        mock_list.return_value = [
            "pre/raw/sp/m/1/views/A/edges/",
            "pre/raw/sp/m/1/views/A.something/edges/",
            "pre/raw/sp/m/1/views/B/edges/",
        ]

        out = test_replicator._edge_folders_for_anchor("sp", "A")

        assert any(x.endswith("/views/A/edges") for x in out)
        assert any("A.something/edges" in x for x in out)
        assert all("B/edges" not in x for x in out)

    def test_atomic_replace_files_cleans_temps_on_failure(
        self, test_replicator, monkeypatch
    ):
        updates = [
            ("s3://bkt/tmp1.parquet", "s3://bkt/final1.parquet", "edges"),
            ("s3://bkt/tmp2.parquet", "s3://bkt/final2.parquet", "nodes"),
        ]
        s3 = Mock()
        test_replicator._s3 = s3
        test_replicator._s3_created_at = datetime.now(UTC)

        def fail_intentionally(*a, **k):
            raise RuntimeError("copy failed")

        monkeypatch.setattr(
            DataModelingReplicator,
            "_copy_and_verify",
            lambda *a, **k: fail_intentionally(),
        )

        with pytest.raises(RuntimeError, match="copy failed"):
            test_replicator._atomic_replace_files(updates)

        keys = {c.kwargs["Key"] for c in s3.delete_object.call_args_list}
        assert keys == {"tmp1.parquet", "tmp2.parquet"}
        assert s3.delete_object.call_count == 2

    @patch.object(DataModelingReplicator, "_delta_append")
    @patch.object(DataModelingReplicator, "_extract_instances")
    def test_send_to_s3(self, mock_extract, mock_append, test_replicator):
        """Test that _send_to_s3 extracts and appends data"""
        mock_extract.return_value = {"table1": [{"data": 1}], "table2": [{"data": 2}]}
        dm_cfg = SimpleNamespace(space="sp")
        result = Mock()

        test_replicator._model_xid = "model"
        test_replicator._model_version = "1"

        test_replicator._send_to_s3(dm_cfg, result)

        mock_extract.assert_called_with(result)
        assert mock_append.call_count == 2
        calls = mock_append.call_args_list
        assert any("table1" in str(call) for call in calls)
        assert any("table2" in str(call) for call in calls)
