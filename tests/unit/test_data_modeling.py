import json
from types import SimpleNamespace
from datetime import datetime

try:  # Py 3.11+
    from datetime import UTC
except ImportError:  # Py 3.10 and earlier
    from datetime import timezone as _tz

    UTC = _tz.utc
import os
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
    stop_event = Mock()
    stop_event.is_set.return_value = False

    r = DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    r.logger = Mock()
    r.s3_cfg = SimpleNamespace(bucket="test-bucket", prefix="pre", region="us-east-1")
    r.config = SimpleNamespace(destination=SimpleNamespace(s3=r.s3_cfg))
    r.state_store = Mock()
    r.cognite_client = Mock()
    r._s3 = Mock()
    r._s3_created_at = datetime.now(UTC)
    r._model_xid = None
    r._model_version = None
    r._expected_node_props = None
    r._view_props_by_xid = {}
    r.stop_event = stop_event
    return r


def mk_node(
    space="sp", xid="n1", ver=1, last=111, created=100, deleted=None, view_xid="viewA"
):
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


def mk_edge(
    space="sp",
    xid="e1",
    ver=1,
    last=111,
    created=100,
    deleted=None,
    type_xid="edgeType",
    start=("sp", "n1"),
    end=("sp", "n2"),
    prop_view="edgeView",
):
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
    view = {
        "space": "vsp",
        "externalId": "vxid",
        "version": 7,
        "properties": {"a": 1, "b": 2},
    }
    q = replicator._node_query_for_view(view)

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

    monkeypatch.setattr(
        "cdf_s3_replicator.data_modeling.ContainerId", fake_container_id
    )

    view = {
        "space": "view_space",
        "externalId": "vx",
        "version": 1,
        "properties": {"p": 1},
    }

    replicator._node_query_for_view(view)

    assert seen["space"] == "view_space"
    assert seen["externalId"] == "vx"


def test_edge_query_for_view_uses_view_space_for_containerid_when_anchor(
    monkeypatch, replicator
):
    seen = {}

    def fake_container_id(space, externalId):
        seen["space"] = space
        seen["externalId"] = externalId
        return (space, externalId)

    monkeypatch.setattr(
        "cdf_s3_replicator.data_modeling.ContainerId", fake_container_id
    )

    view = {
        "space": "view_space",
        "externalId": "vx",
        "version": 3,
        "properties": {"p": 1},
    }

    replicator._edge_query_for_view(view)

    assert seen["space"] == "view_space"
    assert seen["externalId"] == "vx"


def test_edge_query_for_view_node_anchor_builds_expected(replicator):
    view = {"space": "vsp", "externalId": "vx", "version": 3, "properties": {"p": 1}}
    q = replicator._edge_query_for_view(view)
    assert set(q.with_.keys()) == {"nodes", "edges_out", "edges_in"}
    assert "edges_out" in q.select and "edges_in" in q.select


def test_edge_query_for_view_edgeonly_builds_expected(replicator):
    view = {
        "space": "vsp",
        "externalId": "edgeView",
        "version": 1,
        "properties": {"x": 1},
        "usedFor": "edge",
    }
    q = replicator._edge_query_for_view(view)
    assert "edges" in q.with_
    assert "edges" in q.select


# -------------------------
# Core Processing Methods
# -------------------------


def test_process_instances_sets_expected_props_and_calls_iterate(replicator):
    """Test that _process_instances sets expected node props and calls iterate_and_write"""
    dm_cfg = SimpleNamespace(space="sp")
    view = {"space": "sp", "externalId": "v1", "version": 1, "properties": {"p1", "p2"}}

    with patch.object(replicator, "_iterate_and_write") as mock_iterate:
        with patch.object(replicator, "_node_query_for_view") as mock_query:
            replicator._process_instances(dm_cfg, "state_id", view, kind="nodes")

    assert replicator._expected_node_props == {"p1", "p2"}
    assert replicator._view_props_by_xid[("sp", "v1")] == {"p1", "p2"}
    assert mock_iterate.called
    assert mock_query.called


def test_replicate_view_processes_nodes_and_edges(replicator):
    """Test that replicate_view calls _process_instances for both nodes and edges"""
    dm_cfg = SimpleNamespace(space="sp")
    view = {"space": "sp", "externalId": "v1", "version": 1, "properties": {}}

    with patch.object(replicator, "_process_instances") as mock_process:
        replicator._model_xid = "model1"
        replicator._model_version = "1"
        replicator.replicate_view(dm_cfg, "model1", "1", view)

    assert mock_process.call_count == 2
    calls = mock_process.call_args_list
    assert any("nodes" in str(call) for call in calls)
    assert any("edges" in str(call) for call in calls)


@patch.object(DataModelingReplicator, "_get_data_model_views")
@patch.object(DataModelingReplicator, "replicate_view")
def test_process_spaces(mock_replicate, mock_get_views, replicator):
    """Test process_spaces iterates through models and views"""
    replicator.stop_event = Mock()
    replicator.stop_event.is_set.return_value = False

    model = SimpleNamespace(external_id="m1", version=None, views=None)
    dm_cfg = SimpleNamespace(space="sp", data_models=[model])
    replicator.config = SimpleNamespace(data_modeling=[dm_cfg])

    view1 = Mock(
        dump=Mock(return_value={"space": "sp", "externalId": "v1", "version": 1})
    )
    mock_get_views.return_value = ("1", [view1])

    replicator.process_spaces()

    assert mock_get_views.called
    assert mock_replicate.called
    assert replicator._model_xid is None
    assert replicator._model_version is None


# -------------------------
# Data Model Management
# -------------------------


def test_get_data_model_views_explicit_version_not_found_logs_and_empty(
    monkeypatch, replicator, caplog
):
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


def test_get_data_model_views_latest_version_and_subset_with_missing(
    monkeypatch, replicator, caplog
):
    dm_cfg = SimpleNamespace(space="sp")
    model = SimpleNamespace(external_id="m", version=None, views=["vA", "vMissing"])

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


def test_get_data_model_views_explicit_version_no_views(
    monkeypatch, replicator, caplog
):
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
    edges = [
        mk_edge(xid="e1", type_xid="edgeType"),
        mk_edge(xid="e2", type_xid="edgeType"),
    ]
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
    out = replicator._extract_instances(res)

    assert "A/nodes" in out and len(out["A/nodes"]) == 2

    assert "_edges" in out and len(out["_edges"]) == 2

    assert "edgeType/edges" in out and len(out["edgeType/edges"]) == 4
    dirs = {row.get("direction") for row in out["edgeType/edges"]}
    assert dirs == {"in", "out"}


# -------------------------
# Sanitization & schema fallback
# -------------------------


def test_sanitize_rows_empty_input(replicator):
    """Test sanitization handles empty input"""
    assert replicator._sanitize_rows_for_tableau([]) == []


def test_widen_with_props_adds_missing_columns(replicator):
    """Test that _widen_with_props adds missing columns as string type"""
    schema = pa.schema([("existing", pa.string())])
    tbl = pa.Table.from_arrays([pa.array(["val"])], schema=schema)

    result = replicator._widen_with_props(tbl, {"existing", "new1", "new2"})

    assert "new1" in result.column_names
    assert "new2" in result.column_names
    assert result.column("new1").type == pa.string()
    assert result.column("new2").null_count == result.num_rows


def test_coerce_nulltype_fields_to_string(replicator):
    """Test that null type fields are converted to string type"""
    schema = pa.schema([("good", pa.string()), ("null_field", pa.null())])
    tbl = pa.Table.from_arrays(
        [pa.array(["val"]), pa.array([None], type=pa.null())], schema=schema
    )

    result = replicator._coerce_nulltype_fields_to_string(tbl)

    assert result.column("null_field").type == pa.string()
    assert result.column("good").type == pa.string()


@patch("cdf_s3_replicator.data_modeling.DeltaTable")
def test_align_to_existing_schema(mock_dt_cls, replicator):
    """Test schema alignment with existing Delta table"""
    existing_schema = pa.schema(
        [("col1", pa.int64()), ("col2", pa.string()), ("col3", pa.float64())]
    )

    mock_dt = Mock()
    mock_dt.schema().to_pyarrow.return_value = existing_schema
    mock_dt_cls.return_value = mock_dt

    input_schema = pa.schema(
        [
            ("col1", pa.string()),
            ("col2", pa.string()),
        ]
    )
    input_tbl = pa.Table.from_arrays(
        [pa.array(["1"]), pa.array(["val"])], schema=input_schema
    )

    result = replicator._align_to_existing_schema("s3://bucket/table", input_tbl)

    assert mock_dt_cls.called
    _, kwargs = mock_dt_cls.call_args
    assert "storage_options" in kwargs
    assert "col3" in result.column_names
    assert result.column("col3").null_count == result.num_rows


def test_sanitize_rows_for_tableau_mixed_types_and_structs(replicator):
    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "x",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
            "a": {"k": "v"},
            "b": [1, 2, 3],
            "c": b"bytes",
            "d": 1,
        },
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "y",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
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
    """Test that sanitization doesn't drop columns that appear with null values"""
    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "x",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
        },
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "y",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
            "only_in_second": None,
        },
    ]
    out = replicator._sanitize_rows_for_tableau(rows)
    assert "only_in_second" in out[1]
    assert out[1]["only_in_second"] is None


def test_sanitize_handles_mixed_null_columns(replicator):
    """Test sanitization handles columns with mixed null presence"""
    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "x",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
            "all_null": None,
        },
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "y",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
            "all_null": None,
        },
    ]
    out = replicator._sanitize_rows_for_tableau(rows)
    assert all("all_null" not in r or r["all_null"] is None for r in out)


# -------------------------
# Table Creation
# -------------------------


def test_create_empty_tables(replicator):
    """Test creation of empty tables with correct schemas"""
    nodes = replicator._create_empty_nodes_table()
    assert nodes.num_rows == 0
    assert "space" in nodes.column_names
    assert "externalId" in nodes.column_names
    assert "lastUpdatedTime" in nodes.column_names

    edges = replicator._create_empty_edges_table()
    assert edges.num_rows == 0
    assert "startNode.space" in edges.column_names
    assert "endNode.externalId" in edges.column_names


# -------------------------
# Delta Operations
# -------------------------


@patch.object(DataModelingReplicator, "_verify_written_rows", return_value=None)
@patch.object(
    DataModelingReplicator,
    "_validate_tableau_schema",
    side_effect=ValueError("tableShouldFail"),
)
@patch("cdf_s3_replicator.data_modeling.write_deltalake")
def test_delta_append_schema_fallback_to_safe_types(
    mock_write, _mock_validate, _mock_verify, replicator, caplog
):
    replicator._model_xid = "modelA"
    replicator._model_version = "2"
    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "n1",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
            "badcol": b"binarydata",
        }
    ]

    replicator.logger = logging.getLogger(replicator.name)

    with caplog.at_level(logging.WARNING, logger=replicator.name):
        replicator._delta_append("tbl", rows, "sp")

    assert "doing safe types fallback" in caplog.text.lower()
    assert mock_write.called


@patch.object(DataModelingReplicator, "_verify_written_rows", return_value=None)
@patch("cdf_s3_replicator.data_modeling.DeltaTable")
@patch("cdf_s3_replicator.data_modeling.write_deltalake")
def test_delta_append_writes_and_tombstones(
    mock_write, mock_dt_cls, _mock_verify, replicator
):
    replicator._model_xid = "modelA"
    replicator._model_version = "2"
    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "n1",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
        },
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "del_me",
            "version": 1,
            "lastUpdatedTime": 2,
            "createdTime": 1,
            "deletedTime": 123,
        },
    ]
    dt = Mock()
    mock_dt_cls.return_value = dt

    replicator._delta_append("A/nodes", rows, "sp")

    assert "storage_options" in mock_write.call_args.kwargs
    assert "storage_options" in mock_dt_cls.call_args.kwargs

    assert mock_write.call_count == 1
    assert dt.delete.call_count == 1
    predicate = dt.delete.call_args[0][0]
    assert "del_me" in predicate


@patch.object(DataModelingReplicator, "_verify_written_rows", return_value=None)
@patch("cdf_s3_replicator.data_modeling.write_deltalake")
def test_delta_append_without_prefix_has_clean_uri(
    mock_write, _mock_verify, replicator
):
    replicator._model_xid = "m"
    replicator._model_version = "1"
    replicator.s3_cfg.prefix = None

    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "n1",
            "version": 1,
            "lastUpdatedTime": 1,
            "createdTime": 1,
            "deletedTime": None,
        }
    ]
    replicator._delta_append("A/nodes", rows, "sp")

    uri = mock_write.call_args[0][0]
    assert uri == "s3://test-bucket/raw/sp/m/1/views/A/nodes"
    assert "//raw/" not in uri
    assert "storage_options" in mock_write.call_args.kwargs


def test_delta_append_no_model_version_raises(replicator):
    """Test that delta_append raises when no model version is set"""
    replicator._model_xid = "model"
    replicator._model_version = None

    with pytest.raises(RuntimeError, match="No data model version"):
        replicator._delta_append("table", [{"data": 1}], "space")


def test_delete_tombstones_chunks(replicator):
    dt = Mock()
    tombstones = [f"id{i}" for i in range(12)]
    replicator._delete_tombstones(dt, tombstones, chunk=5)

    assert dt.delete.call_count == 3

    preds = [c.args[0] for c in dt.delete.call_args_list]
    assert not any(all(f"id{i}" in p for i in range(12)) for p in preds)
    assert any("id0" in preds[0] and "id5" not in preds[0] for _ in [0])


# -------------------------
# Iteration and Sync
# -------------------------


@patch("cdf_s3_replicator.data_modeling.DataModelingReplicator._send_to_s3")
def test_iterate_and_write_cursor_expired_and_recovery(mock_send, replicator):
    replicator.state_store.get_state.return_value = (None, '{"a": "b"}')
    err = CogniteAPIError(message="cursor has expired", code=400)
    fake_node = SimpleNamespace(last_updated_time=1234567890)
    second = SimpleNamespace(data={"nodes": [fake_node]}, cursors={"next": 1})
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

    page1 = SimpleNamespace(
        data={"nodes": [SimpleNamespace(last_updated_time=0)]}, cursors={"c": 1}
    )
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
    replicator.state_store.set_state.assert_any_call(
        external_id="sid", high=json.dumps({"c": 1})
    )


@patch("cdf_s3_replicator.data_modeling.DataModelingReplicator._send_to_s3")
def test_iterate_and_write_short_circuits_on_future_change(
    mock_send, replicator, monkeypatch
):
    monkeypatch.setattr("time.time", lambda: 1000.0)  # query_start_ms = 1_000_000

    page1 = SimpleNamespace(
        data={"nodes": [SimpleNamespace(last_updated_time=1_000_000)]},
        cursors={"c": 1},
    )
    page2 = SimpleNamespace(
        data={"nodes": [SimpleNamespace(last_updated_time=0)]}, cursors=None
    )

    replicator.state_store.get_state.return_value = (None, None)
    replicator._safe_sync = Mock(side_effect=[page1, page2])

    q = SimpleNamespace(cursors=None)
    dm_cfg = SimpleNamespace(space="sp")

    replicator._iterate_and_write(dm_cfg, "sid", q)

    assert mock_send.call_count == 1
    assert replicator._safe_sync.call_count == 1


def test_page_contains_future_change(replicator):
    """Test detection of future changes in page"""
    t0_ms = 1000

    future_node = SimpleNamespace(last_updated_time=2000)
    res = SimpleNamespace(data={"nodes": [future_node]})
    assert replicator._page_contains_future_change(res, t0_ms) is True

    past_node = SimpleNamespace(last_updated_time=500)
    res = SimpleNamespace(data={"nodes": [past_node]})
    assert replicator._page_contains_future_change(res, t0_ms) is False


# -------------------------
# Snapshot Publishing
# -------------------------


@patch("cdf_s3_replicator.data_modeling.pq.write_table")
@patch("cdf_s3_replicator.data_modeling.DeltaTable")
@patch.object(DataModelingReplicator, "_atomic_replace_files")
@patch.object(DataModelingReplicator, "_edge_folders_for_anchor", return_value=[])
def test_write_view_snapshot_smoke(
    mock_edge_folders, mock_atomic, mock_delta, mock_write, replicator, tmp_path
):
    replicator._model_xid = "m"
    replicator._model_version = "1"

    tbl = Mock()
    tbl.to_pyarrow_table.return_value = pa.Table.from_pylist([])
    tbl.files.return_value = []
    mock_delta.return_value = tbl

    replicator._write_view_snapshot("sp", "A", is_edge_only=False)

    assert mock_atomic.call_count == 1
    assert len(mock_write.call_args_list) >= 2  # edges + nodes written
    for _, kwargs in mock_delta.call_args_list:
        assert "storage_options" in kwargs
    assert len(mock_delta.call_args_list) >= 1  # nodes DeltaTable check


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
    for _, kwargs in mock_delta.call_args_list:
        assert "storage_options" in kwargs
    assert len(mock_delta.call_args_list) >= 1  # edges DeltaTable check


@patch("cdf_s3_replicator.data_modeling.pq.write_table")
@patch.object(DataModelingReplicator, "_atomic_replace_files")
@patch.object(DataModelingReplicator, "_get_data_model_views")
def test_publish_space_snapshots_full_flow(
    mock_get_views, mock_atomic, mock_write, replicator
):
    """Test full snapshot publishing flow"""
    model = SimpleNamespace(external_id="m1", version=1, views=None)
    dm_cfg = SimpleNamespace(space="sp", data_models=[model])

    view = SimpleNamespace(external_id="v1", version=1, used_for="node")
    mock_get_views.return_value = ("1", [view])

    with patch.object(replicator, "_write_view_snapshot") as mock_write_snapshot:
        replicator._publish_space_snapshots(dm_cfg)

    assert mock_write_snapshot.called
    assert replicator._model_version is None


@patch("cdf_s3_replicator.data_modeling.DeltaTable")
def test_dedup_latest_nodes_keeps_newest_per_space_externalid(mock_dt, replicator):
    schema = pa.schema(
        [
            ("space", pa.string()),
            ("externalId", pa.string()),
            ("lastUpdatedTime", pa.int64()),
        ]
    )

    b1 = pa.record_batch(
        [pa.array(["s", "s"]), pa.array(["a", "b"]), pa.array([20, 5])], schema=schema
    )
    b2 = pa.record_batch(
        [pa.array(["s", "s"]), pa.array(["a", "c"]), pa.array([10, 7])], schema=schema
    )

    class _FakeDataset:
        def to_batches(self, *args, **kwargs):
            return [b1, b2]

    fake_table = Mock()
    fake_table.to_pyarrow_dataset.return_value = _FakeDataset()
    mock_dt.return_value = fake_table

    out = replicator._dedup_latest_nodes("ignored")

    args, kwargs = mock_dt.call_args
    assert "storage_options" in kwargs
    rows = set(
        zip(
            out.column("externalId").to_pylist(),
            out.column("lastUpdatedTime").to_pylist(),
        )
    )
    assert rows == {("a", 20), ("b", 5), ("c", 7)}


# -------------------------
# Environment / Mode switching
# -------------------------
def test_init_sets_imds_disabled_in_dev(monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.delenv("AWS_EC2_METADATA_DISABLED", raising=False)
    DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    assert os.getenv("AWS_EC2_METADATA_DISABLED") == "true"


def test_init_unsets_imds_in_prod(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    assert os.getenv("AWS_EC2_METADATA_DISABLED") is None


def test_s3_storage_options_dev_includes_keys_prod_omits(monkeypatch):
    # shared env
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA_TEST")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    # DEV
    monkeypatch.setenv("APP_ENV", "dev")
    r_dev = DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    r_dev.logger = Mock()  # silence logs
    opts_dev = r_dev._s3_storage_options()
    assert opts_dev["AWS_REGION"] == "us-east-1"
    assert opts_dev["AWS_ACCESS_KEY_ID"] == "AKIA_TEST"
    assert opts_dev["AWS_SECRET_ACCESS_KEY"] == "secret"

    # PROD
    monkeypatch.setenv("APP_ENV", "prod")
    r_prod = DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    r_prod.logger = Mock()
    opts_prod = r_prod._s3_storage_options()
    assert opts_prod.get("AWS_REGION") == "us-east-1"
    assert "AWS_ACCESS_KEY_ID" not in opts_prod
    assert "AWS_SECRET_ACCESS_KEY" not in opts_prod


# -------------------------
# Configuration Management
# -------------------------


def test_reload_remote_config_success(replicator):
    """Test successful remote config reload"""
    replicator.config = SimpleNamespace(
        cognite=Mock(),
        destination=SimpleNamespace(s3=SimpleNamespace(bucket="new-bucket")),
    )

    with patch(
        "cdf_s3_replicator.data_modeling.CdfExtractorConfig.retrieve_pipeline_config"
    ) as mock_retrieve:
        new_config = SimpleNamespace(
            destination=SimpleNamespace(s3=SimpleNamespace(bucket="updated-bucket"))
        )
        mock_retrieve.return_value = new_config

        result = replicator._reload_remote_config()

    assert result is True
    assert replicator.s3_cfg.bucket == "updated-bucket"


# -------------------------
# Utility Functions
# -------------------------


def test_convert_to_safe_types_core_int_fields_bad_values_become_none(replicator):
    rows = [
        {
            "space": "sp",
            "instanceType": "node",
            "externalId": "x",
            "version": "v1",
            "lastUpdatedTime": "nope",
            "createdTime": 123,
            "deletedTime": None,
            "someProp": {"a": 1},
        }
    ]
    tbl = replicator._convert_to_safe_types(rows)
    cols = {f.name: f.type for f in tbl.schema}
    assert str(cols["version"]) == "int64"
    assert str(cols["lastUpdatedTime"]) == "int64"
    assert tbl.column("version").to_pylist()[0] is None
    assert tbl.column("lastUpdatedTime").to_pylist()[0] is None
    assert str(cols["someProp"]) == "string"


def test_should_retry_exc_behaviour():
    assert dm._should_retry_exc(RequestException()) is True
    ce = CogniteAPIError(message="cursor has expired", code=400)
    assert dm._should_retry_exc(ce) is False
