import json
from types import SimpleNamespace
from datetime import datetime, UTC
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest
import logging

from cdf_s3_replicator.data_modeling import DataModelingReplicator
from cognite.client.exceptions import CogniteAPIError
from requests.exceptions import RequestException
from cdf_s3_replicator import data_modeling as dm

# -------------------------
# Fixtures / helpers
# -------------------------

class _ViewKey:
    def __init__(self, external_id):
        self.external_id = external_id
    def __hash__(self):
        return hash(self.external_id)
    def __eq__(self, other):
        return getattr(other, "external_id", None) == self.external_id


@pytest.fixture
def replicator():
    r = DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    r.logger = Mock()
    r.s3_cfg = SimpleNamespace(bucket="test-bucket", prefix="pre", region="us-east-1")
    r.config = SimpleNamespace(destination=SimpleNamespace(s3=r.s3_cfg))
    r.state_store = Mock()
    r.cognite_client = Mock()
    r._s3 = Mock()
    r._s3_created_at = datetime.now(UTC)
    return r


def mk_node(space="sp", xid="n1", ver=1, last=111, created=100, deleted=None, view_xid="viewA"):
    props_data = {_ViewKey(view_xid): {"p1": "v1", "p2": 2}}
    properties = SimpleNamespace(data=props_data)
    return SimpleNamespace(
        space=space,
        external_id=xid,
        version=ver,
        last_updated_time=last,
        created_time=created,
        deleted_time=deleted,
        properties=properties,
    )


def mk_edge(space="sp", xid="e1", ver=1, last=111, created=100, deleted=None, type_xid="edgeType",
            start=("sp", "n1"), end=("sp", "n2"), prop_view="edgeView"):
    start_node = SimpleNamespace(space=start[0], external_id=start[1])
    end_node = SimpleNamespace(space=end[0], external_id=end[1])
    type_obj = SimpleNamespace(external_id=type_xid)
    props_data = {_ViewKey(prop_view): {"ep": 42}}
    properties = SimpleNamespace(data=props_data)
    return SimpleNamespace(
        space=space,
        external_id=xid,
        version=ver,
        last_updated_time=last,
        created_time=created,
        deleted_time=deleted,
        start_node=start_node,
        end_node=end_node,
        type=type_obj,
        properties=properties,
    )


# -------------------------
# Query builders
# -------------------------

def test_node_query_for_view_builds_expected(replicator):
    dm_cfg = SimpleNamespace(space="sp")
    view = {"space": "vsp", "externalId": "vxid", "version": 7, "properties": {"a": 1, "b": 2}}
    q = replicator._node_query_for_view(dm_cfg, view)

    assert "nodes" in q.with_
    assert q.with_["nodes"].limit == 2000
    assert "nodes" in q.select

    qd = q.dump()
    assert "nodes" in qd["select"]

    srcs = qd["select"]["nodes"]["sources"]
    assert len(srcs) == 1
    src = srcs[0]["source"]
    assert src["space"] == "vsp"
    assert src["externalId"] == "vxid"
    assert src["version"] == 7
    assert set(srcs[0]["properties"]) == {"a", "b"}


def test_node_query_uses_view_space_for_containerid(monkeypatch, replicator):
    seen = {}

    def fake_container_id(space, externalId):
        seen["space"] = space
        seen["externalId"] = externalId
        return (space, externalId)

    monkeypatch.setattr("cdf_s3_replicator.data_modeling.ContainerId", fake_container_id)

    dm_cfg = SimpleNamespace(space="cfg_space")
    view = {"space": "view_space", "externalId": "vx", "version": 1, "properties": {"p": 1}}

    replicator._node_query_for_view(dm_cfg, view)

    assert seen["space"] == "view_space"
    assert seen["externalId"] == "vx"


def test_edge_query_for_view_uses_view_space_for_containerid_when_anchor(monkeypatch, replicator):
    seen = {}

    def fake_container_id(space, externalId):
        seen["space"] = space
        seen["externalId"] = externalId
        return (space, externalId)

    monkeypatch.setattr("cdf_s3_replicator.data_modeling.ContainerId", fake_container_id)

    dm_cfg = SimpleNamespace(space="cfg_space")
    view = {"space": "view_space", "externalId": "vx", "version": 3, "properties": {"p": 1}}

    replicator._edge_query_for_view(dm_cfg, view)

    assert seen["space"] == "view_space"
    assert seen["externalId"] == "vx"


def test_edge_query_for_view_node_anchor_builds_expected(replicator):
    dm_cfg = SimpleNamespace(space="sp")
    view = {"space": "vsp", "externalId": "vx", "version": 3, "properties": {"p": 1}}
    q = replicator._edge_query_for_view(dm_cfg, view)
    assert set(q.with_.keys()) == {"nodes", "edges_out", "edges_in"}
    assert "edges_out" in q.select and "edges_in" in q.select


def test_edge_query_for_view_edgeonly_builds_expected(replicator):
    dm_cfg = SimpleNamespace(space="sp")
    view = {"space": "vsp", "externalId": "edgeView", "version": 1, "properties": {"x": 1}, "usedFor": "edge"}
    q = replicator._edge_query_for_view(dm_cfg, view)
    assert "edges" in q.with_
    assert "edges" in q.select


def test_get_data_model_views_explicit_version_not_found_logs_and_empty(monkeypatch, replicator, caplog):
    dm_cfg = SimpleNamespace(space="sp")
    model = SimpleNamespace(external_id="m", version=3, views=None)

    replicator.logger = logging.getLogger(replicator.name)

    cc = replicator.cognite_client
    cc.data_modeling.data_models.retrieve.return_value = []

    with caplog.at_level(logging.WARNING, logger=replicator.name):
        ver, views = replicator._get_data_model_views(dm_cfg, model)

    assert ver == "3"
    assert views == []
    assert "not found" in caplog.text.lower()


def test_get_data_model_views_latest_version_and_subset_with_missing(monkeypatch, replicator, caplog):
    dm_cfg = SimpleNamespace(space="sp")
    model = SimpleNamespace(external_id="m", version=None, views=["vA","vMissing"])

    replicator.logger = logging.getLogger(replicator.name)

    cc = replicator.cognite_client

    latest_model = SimpleNamespace(
        external_id="m",
        version=9,
        views=[
            SimpleNamespace(space="sp", external_id="vA", version=1),
            SimpleNamespace(space="sp", external_id="vB", version=2),
        ],
    )
    cc.data_modeling.data_models.list.return_value = [latest_model]

    retrieved_view = SimpleNamespace(external_id="vA", version=1, used_for="node")
    cc.data_modeling.views.retrieve.return_value = [retrieved_view]

    with caplog.at_level(logging.WARNING, logger=replicator.name):
        ver, views = replicator._get_data_model_views(dm_cfg, model)

    assert ver == "9"
    assert views == [retrieved_view]
    assert "not found" in caplog.text.lower()


def test_get_data_model_views_explicit_version_no_views(monkeypatch, replicator, caplog):
    dm_cfg = SimpleNamespace(space="sp")
    model = SimpleNamespace(external_id="m", version=2, views=None)

    replicator.logger = logging.getLogger(replicator.name)

    cc = replicator.cognite_client
    found = SimpleNamespace(external_id="m", version=2, views=[])
    cc.data_modeling.data_models.retrieve.return_value = [found]

    with caplog.at_level(logging.WARNING, logger=replicator.name):
        ver, views = replicator._get_data_model_views(dm_cfg, model)

    assert ver == "2"
    assert views == []
    assert "has no views" in caplog.text.lower()


# -------------------------
# Extract instances
# -------------------------

def test_extract_instances_nodes_and_edges_dirs(replicator):
    nodes = [mk_node(xid="n1", view_xid="A"), mk_node(xid="n2", view_xid="A")]
    edges = [mk_edge(xid="e1", type_xid="edgeType"),
             mk_edge(xid="e2", type_xid="edgeType")]
    edges_out = [mk_edge(xid="eo1"), mk_edge(xid="eo2")]
    edges_in = [mk_edge(xid="ei1"), mk_edge(xid="ei2")]
    res = SimpleNamespace(
        data={
            "nodes": nodes,
            "edges": edges,
            "edges_out": edges_out,
            "edges_in": edges_in,
        }
    )
    out = DataModelingReplicator._extract_instances(res)

    assert "A/nodes" in out and len(out["A/nodes"]) == 2

    assert "_edges" in out and len(out["_edges"]) == 2

    assert "edgeType/edges" in out and len(out["edgeType/edges"]) == 4
    dirs = {row.get("direction") for row in out["edgeType/edges"]}
    assert dirs == {"in", "out"}


# -------------------------
# Sanitization & schema fallback
# -------------------------

def test_sanitize_rows_for_tableau_mixed_types_and_structs(replicator):
    rows = [
        {
            "space": "sp", "instanceType": "node", "externalId": "x", "version": 1,
            "lastUpdatedTime": 1, "createdTime": 1, "deletedTime": None,
            "a": {"k": "v"},
            "b": [1, 2, 3],
            "c": b"bytes",
            "d": 1,
        },
        {
            "space": "sp", "instanceType": "node", "externalId": "y", "version": 1,
            "lastUpdatedTime": 1, "createdTime": 1, "deletedTime": None,
            "a": None,
            "b": None,
            "c": "text",
            "d": "string-here",
        },
    ]
    out = replicator._sanitize_rows_for_tableau(rows)
    assert isinstance(out[0]["a"], (str, type(None)))
    assert isinstance(out[0]["b"], (str, type(None)))
    assert out[0]["c"] == "bytes"
    assert isinstance(out[0]["d"], str) and isinstance(out[1]["d"], str)


def test_sanitize_drops_columns_null_in_all_rows(replicator):
    rows = [
        {"space": "sp", "instanceType": "node", "externalId": "x", "version": 1,
         "lastUpdatedTime": 1, "createdTime": 1, "deletedTime": None},
        {"space": "sp", "instanceType": "node", "externalId": "y", "version": 1,
         "lastUpdatedTime": 1, "createdTime": 1, "deletedTime": None, "only_in_second": None},
    ]
    out = replicator._sanitize_rows_for_tableau(rows)
    assert all("only_in_second" not in r for r in out)


@patch.object(DataModelingReplicator, "_validate_tableau_schema", side_effect=ValueError("tableShouldFail"))
@patch("cdf_s3_replicator.data_modeling.write_deltalake")
def test_delta_append_schema_fallback_to_safe_types(mock_write, _mock_validate, replicator, caplog):
    replicator._model_xid = "modelA"
    replicator._model_version = "2"
    rows = [{
        "space": "sp", "instanceType": "node", "externalId": "n1",
        "version": 1, "lastUpdatedTime": 1, "createdTime": 1, "deletedTime": None,
        "badcol": b"binarydata"
    }]

    replicator.logger = logging.getLogger(replicator.name)

    with caplog.at_level(logging.WARNING, logger=replicator.name):
        replicator._delta_append("tbl", rows, "sp")

    assert "doing safe types fallback" in caplog.text.lower()
    assert mock_write.called


def test_convert_to_safe_types_core_int_fields_bad_values_become_none(replicator):
    rows = [{
        "space": "sp",
        "instanceType": "node",
        "externalId": "x",
        "version": "v1",
        "lastUpdatedTime": "nope",
        "createdTime": 123,
        "deletedTime": None,
        "someProp": {"a": 1},
    }]
    tbl = replicator._convert_to_safe_types(rows)
    cols = {f.name: f.type for f in tbl.schema}
    assert str(cols["version"]) == "int64"
    assert str(cols["lastUpdatedTime"]) == "int64"
    assert tbl.column("version").to_pylist()[0] is None
    assert tbl.column("lastUpdatedTime").to_pylist()[0] is None
    assert str(cols["someProp"]) == "string"


@patch("cdf_s3_replicator.data_modeling.write_deltalake")
@patch("cdf_s3_replicator.data_modeling.DeltaTable")
def test_delta_append_writes_and_tombstones(mock_dt_cls, mock_write, replicator):
    replicator._model_xid = "modelA"
    replicator._model_version = "2"
    rows = [
        {"space": "sp", "instanceType": "node", "externalId": "n1",
         "version": 1, "lastUpdatedTime": 1, "createdTime": 1, "deletedTime": None},
        {"space": "sp", "instanceType": "node", "externalId": "del_me",
         "version": 1, "lastUpdatedTime": 2, "createdTime": 1, "deletedTime": 123},
    ]
    dt = Mock()
    mock_dt_cls.return_value = dt

    replicator._delta_append("A/nodes", rows, "sp")

    assert mock_write.call_count == 1
    args, kwargs = mock_write.call_args
    assert args[0].startswith("s3://test-bucket/pre/raw/sp/modelA/2/views/A/nodes")
    assert dt.delete.call_count == 1
    predicate = dt.delete.call_args[0][0]
    assert "del_me" in predicate


@patch("cdf_s3_replicator.data_modeling.write_deltalake")
def test_delta_append_without_prefix_has_clean_uri(mock_write, replicator):
    replicator._model_xid = "m"
    replicator._model_version = "1"
    replicator.s3_cfg.prefix = None

    rows = [{"space": "sp","instanceType":"node","externalId":"n1","version":1,"lastUpdatedTime":1,"createdTime":1,"deletedTime":None}]
    replicator._delta_append("A/nodes", rows, "sp")

    uri = mock_write.call_args[0][0]
    assert uri == "s3://test-bucket/raw/sp/m/1/views/A/nodes"
    assert "//raw/" not in uri


def test_delete_tombstones_chunks(replicator):
    dt = Mock()
    tombstones = [f"id{i}" for i in range(12)]
    replicator._delete_tombstones(dt, tombstones, chunk=5)

    assert dt.delete.call_count == 3

    preds = [c.args[0] for c in dt.delete.call_args_list]
    assert not any(all(f"id{i}" in p for i in range(12)) for p in preds)
    assert any("id0" in preds[0] and "id5" not in preds[0] for _ in [0])


@patch("cdf_s3_replicator.data_modeling.DataModelingReplicator._send_to_s3")
def test_iterate_and_write_cursor_expired_and_recovery(mock_send, replicator):
    replicator.state_store.get_state.return_value = (None, '{"a": "b"}')
    err = CogniteAPIError(message="cursor has expired", code=400)
    fake_node = SimpleNamespace(last_updated_time=1234567890)
    second = SimpleNamespace(
        data={"nodes": [fake_node]},
        cursors={"next": 1}
    )
    replicator._safe_sync = Mock(
        side_effect=[
            err,
            second,
            SimpleNamespace(data={"nodes": []}, cursors=None),
        ]
    )

    q = SimpleNamespace(cursors={"a": "b"})
    dm_cfg = SimpleNamespace(space="sp")

    replicator._iterate_and_write(dm_cfg, "state_id", q)

    replicator.state_store.set_state.assert_any_call(external_id="state_id", high=None)
    assert mock_send.call_count >= 1


@patch("cdf_s3_replicator.data_modeling.DataModelingReplicator._send_to_s3")
def test_iterate_and_write_cursor_expired_mid_pagination(mock_send, replicator):
    replicator.state_store.get_state.return_value = (None, None)

    page1 = SimpleNamespace(data={"nodes": [SimpleNamespace(last_updated_time=0)]}, cursors={"c": 1})
    err = CogniteAPIError(message="cursor has expired", code=400)

    replicator._safe_sync = Mock(side_effect=[page1, err])

    q = SimpleNamespace(cursors=None)
    dm_cfg = SimpleNamespace(space="sp")

    replicator._iterate_and_write(dm_cfg, "sid", q)

    assert mock_send.call_count == 1
    replicator.state_store.set_state.assert_any_call(external_id="sid", high=None)


@patch("cdf_s3_replicator.data_modeling.DataModelingReplicator._send_to_s3")
def test_iterate_and_write_pagination_updates_state(mock_send, replicator):
    replicator.state_store.get_state.return_value = (None, None)

    fake_node = SimpleNamespace(last_updated_time=0)

    page1 = SimpleNamespace(data={"nodes": [fake_node]}, cursors={"c": 1})
    page2 = SimpleNamespace(data={"nodes": []}, cursors=None)
    replicator._safe_sync = Mock(side_effect=[page1, page2])

    q = SimpleNamespace(cursors=None)
    dm_cfg = SimpleNamespace(space="sp")

    replicator._iterate_and_write(dm_cfg, "sid", q)

    assert mock_send.call_count == 1
    replicator.state_store.set_state.assert_any_call(external_id="sid", high=json.dumps({"c": 1}))


@patch("cdf_s3_replicator.data_modeling.DataModelingReplicator._send_to_s3")
def test_iterate_and_write_short_circuits_on_future_change(mock_send, replicator, monkeypatch):
    monkeypatch.setattr("time.time", lambda: 1000.0)  # query_start_ms = 1_000_000

    page1 = SimpleNamespace(
        data={"nodes": [SimpleNamespace(last_updated_time=1_000_000)]},
        cursors={"c": 1},
    )
    page2 = SimpleNamespace(data={"nodes": [SimpleNamespace(last_updated_time=0)]}, cursors=None)

    replicator.state_store.get_state.return_value = (None, None)
    replicator._safe_sync = Mock(side_effect=[page1, page2])

    q = SimpleNamespace(cursors=None)
    dm_cfg = SimpleNamespace(space="sp")

    replicator._iterate_and_write(dm_cfg, "sid", q)

    assert mock_send.call_count == 1
    assert replicator._safe_sync.call_count == 1


def test_get_s3_prefix_requires_version(replicator):
    replicator._model_xid = "m"
    replicator._model_version = None
    with pytest.raises(RuntimeError):
        replicator._get_s3_prefix("space", "raw")


@patch.object(DataModelingReplicator, "_list_prefixes")
def test_edge_folders_for_anchor_filters_correctly(mock_list, replicator):
    replicator._model_xid = "m"
    replicator._model_version = "1"
    mock_list.return_value = [
        "pre/raw/sp/m/1/views/A/edges/",
        "pre/raw/sp/m/1/views/A.something/edges/",
        "pre/raw/sp/m/1/views/B/edges/",
    ]
    out = replicator._edge_folders_for_anchor("sp", "A")
    assert any(x.endswith("/views/A/edges") for x in out)
    assert any("/views/A.something/edges" in x for x in out)
    assert all("/views/B/edges" not in x for x in out)


def test_ensure_s3_recreates_client_after_50_minutes(monkeypatch, replicator):
    first = object()
    second = object()

    calls = {"n": 0}
    def fake_make():
        calls["n"] += 1
        return first if calls["n"] == 1 else second

    monkeypatch.setattr(DataModelingReplicator, "_make_s3_client", lambda self: fake_make())

    replicator._s3 = None
    replicator._s3_created_at = None
    replicator._ensure_s3()
    assert replicator._s3 is first

    replicator._s3_created_at = datetime(2020, 1, 1, tzinfo=UTC)  # very old
    replicator._ensure_s3()
    assert replicator._s3 is second


@patch("cdf_s3_replicator.data_modeling.pq.write_table")
@patch("cdf_s3_replicator.data_modeling.DeltaTable")
@patch.object(DataModelingReplicator, "_atomic_replace_files")
@patch.object(DataModelingReplicator, "_edge_folders_for_anchor", return_value=[])  # <— add this
def test_write_view_snapshot_smoke(mock_atomic, mock_delta, mock_write, replicator, tmp_path):
    replicator._model_xid = "m"
    replicator._model_version = "1"

    tbl = Mock()
    tbl.to_pyarrow_table.return_value = pa.Table.from_pylist([])
    tbl.files.return_value = []
    mock_delta.return_value = tbl

    replicator._write_view_snapshot("sp", "A", is_edge_only=False)

    assert mock_atomic.call_count == 1
    assert len(mock_write.call_args_list) >= 2


@patch("cdf_s3_replicator.data_modeling.pq.write_table")
@patch("cdf_s3_replicator.data_modeling.DeltaTable")
@patch.object(DataModelingReplicator, "_atomic_replace_files")
def test_write_view_snapshot_edge_only(mock_atomic, mock_delta, mock_write, replicator):
    replicator._model_xid = "m"
    replicator._model_version = "1"

    tbl = Mock()
    tbl.to_pyarrow_table.return_value = pa.Table.from_pylist([])
    mock_delta.return_value = tbl

    replicator._write_view_snapshot("sp", "A", is_edge_only=True)

    written_paths = [args[1] for args, _ in mock_write.call_args_list]
    assert all("nodes" not in p for p in written_paths)


def test_atomic_replace_files_cleans_temps_on_failure(replicator, monkeypatch):
    updates = [
        ("s3://bkt/tmp1.parquet", "s3://bkt/final1.parquet", "edges"),
        ("s3://bkt/tmp2.parquet", "s3://bkt/final2.parquet", "nodes"),
    ]
    s3 = Mock()
    replicator._s3 = s3

    def fail_intentionally(*a, **k):
        raise RuntimeError("copy failed")
    monkeypatch.setattr(DataModelingReplicator, "_copy_and_verify", lambda *a, **k: fail_intentionally())

    with pytest.raises(RuntimeError, match="copy failed"):
        replicator._atomic_replace_files(updates)

    keys = {c.kwargs["Key"] for c in s3.delete_object.call_args_list}
    assert keys == {"tmp1.parquet", "tmp2.parquet"}
    assert s3.delete_object.call_count == 2


@patch("cdf_s3_replicator.data_modeling.DeltaTable")
def test_dedup_latest_nodes_keeps_newest_per_space_externalid(mock_dt, replicator):
    schema = pa.schema([
        ("space", pa.string()),
        ("externalId", pa.string()),
        ("lastUpdatedTime", pa.int64()),
    ])

    b1 = pa.record_batch(
        [pa.array(["s","s"]),
         pa.array(["a","b"]),
         pa.array([20, 5])],
        schema=schema
    )
    b2 = pa.record_batch(
        [pa.array(["s","s"]),
         pa.array(["a","c"]),
         pa.array([10, 7])],
        schema=schema
    )

    class _FakeDataset:
        def to_batches(self): return [b1, b2]

    fake_table = Mock()
    fake_table.to_pyarrow_dataset.return_value = _FakeDataset()
    mock_dt.return_value = fake_table

    out = replicator._dedup_latest_nodes("ignored")

    rows = set(zip(out.column("externalId").to_pylist(),
                   out.column("lastUpdatedTime").to_pylist()))
    assert rows == {("a", 20), ("b", 5), ("c", 7)}


def test_should_retry_exc_behaviour():
    assert dm._should_retry_exc(RequestException()) is True
    ce = CogniteAPIError(message="cursor has expired", code=400)
    assert dm._should_retry_exc(ce) is False
