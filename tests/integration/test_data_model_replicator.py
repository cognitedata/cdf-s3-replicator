import os
import time
import pytest
import pandas as pd
from types import SimpleNamespace
from unittest.mock import Mock
from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space, SpaceApply, DataModel, View
from cognite.client.data_classes.data_modeling.ids import DataModelId

from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get

from cdf_s3_replicator.metrics import Metrics
from cdf_s3_replicator.data_modeling import DataModelingReplicator

from tests.integration.integration_steps.cdf_steps import (
    apply_data_model_instances_in_cdf,
)
from tests.integration.integration_steps.s3_steps import (
    delete_delta_table_data,
    assert_data_model_instances_in_s3,
    get_delta_table,
    DATA_MODEL_TIMESTAMP_COLUMNS,
)

from tests.integration.integration_steps.data_model_generation import (
    Node,
    Edge,
    create_node,
    create_edge,
)

RESOURCES = Path(__file__).parent / "resources"


# -------------------------
# Helpers
# -------------------------

def _raw_views_base(replicator: DataModelingReplicator, space: str, model_xid: str, model_version: str) -> str:
    """
    s3://<bucket>/<optional-prefix>raw/<space>/<model>/<version>/views
    """
    s3 = replicator.s3_cfg
    assert s3 is not None and s3.bucket, "replicator.s3_cfg must be initialized"
    prefix = (s3.prefix.rstrip('/') + '/') if s3.prefix else ''
    return f"s3://{s3.bucket}/{prefix}raw/{space}/{model_xid}/{model_version}/views"


# -------------------------
# Fixtures
# -------------------------

@pytest.fixture(scope="function")
def dm_replicator(test_config) -> DataModelingReplicator:
    stop_event = CancellationToken()
    r = DataModelingReplicator(metrics=safe_get(Metrics), stop_event=stop_event)
    r._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    r.cognite_client = r.config.cognite.get_cognite_client(r.name)
    r._load_state_store()
    r.logger = Mock()

    r.s3_cfg = r.config.destination.s3
    assert r.s3_cfg and r.s3_cfg.bucket, "destination.s3 must be configured"

    return r


@pytest.fixture(scope="session")
def test_space(test_config, cognite_client: CogniteClient):
    space_id = test_config["data_modeling"][0]["space"]
    space = cognite_client.data_modeling.spaces.retrieve(space_id)
    if space is None:
        new_space = SpaceApply(
            space_id,
            name="Integration Test Space",
            description="The space used for integration tests.",
        )
        space = cognite_client.data_modeling.spaces.apply(new_space)
    yield space


@pytest.fixture(scope="function")
def test_model(
    cognite_client: CogniteClient,
    test_space: Space,
):
    test_dml = (RESOURCES / "movie_model.graphql").read_text()
    movie_id = DataModelId(space=test_space.space, external_id="Movie", version="1")
    created = cognite_client.data_modeling.graphql.apply_dml(
        id=movie_id,
        dml=test_dml,
        name="Movie Model",
        description="The Movie Model used in Integration Tests",
    )
    models = cognite_client.data_modeling.data_models.retrieve(
        created.as_id(), inline_views=True
    )
    yield models.latest_version()

    cognite_client.data_modeling.data_models.delete(ids=[movie_id])
    views = models.data[0].views
    for view in views:
        cognite_client.data_modeling.views.delete(
            (test_space.space, view.external_id, view.version)
        )
        cognite_client.data_modeling.containers.delete(
            (test_space.space, view.external_id)
        )


@pytest.fixture(scope="function")
def instance_table_paths(
    test_model: DataModel[View], dm_replicator: DataModelingReplicator
):
    """
    Build RAW node table paths for the configured views and clear them up-front.
    """
    cfg_views = dm_replicator.config.data_modeling[0].data_models[0].views or []
    model_xid = test_model.external_id
    model_ver = str(test_model.version)
    base = _raw_views_base(dm_replicator, test_model.space, model_xid, model_ver)

    paths = []
    for view_xid in cfg_views:
        nodes_path = f"{base}/{view_xid}/nodes"
        delete_delta_table_data(None, nodes_path)
        paths.append(nodes_path)

    yield paths

    for p in paths:
        delete_delta_table_data(None, p)


@pytest.fixture(scope="function")
def example_actor():
    return Node(
        "arnold_schwarzenegger",
        "Actor",
        {
            "Actor": {"wonOscar": False},
            "Person": {"name": "Arnold Schwarzenegger", "birthYear": 1947},
        },
    )


@pytest.fixture(scope="function")
def updated_actor():
    return Node("arnold_schwarzenegger", "Actor", {"Actor": {"wonOscar": True}})


@pytest.fixture(scope="function")
def example_movie():
    return Node(
        "terminator",
        "Movie",
        {"Movie": {"title": "Terminator", "releaseYear": 1984}},
    )


@pytest.fixture(scope="function")
def example_edge_actor_to_movie(example_actor, example_movie):
    return Edge(
        "relation:arnold_schwarzenegger:terminator",
        "movies",
        example_actor,
        example_movie,
    )


@pytest.fixture(scope="function")
def example_edge_movie_to_actor(example_actor, example_movie):
    return Edge(
        "relation:terminator:arnold_schwarzenegger",
        "actors",
        example_movie,
        example_actor,
    )


@pytest.fixture(scope="function")
def node_list(
    test_model: DataModel[View],
    example_actor: Node,
    example_movie: Node,
    cognite_client: CogniteClient,
):
    yield [
        create_node(test_model.space, example_actor, test_model),
        create_node(test_model.space, example_movie, test_model),
    ]
    cognite_client.data_modeling.instances.delete(
        nodes=[
            (test_model.space, example_actor.external_id),
            (test_model.space, example_movie.external_id),
        ]
    )


@pytest.fixture(scope="function")
def updated_node_list(
    test_model: DataModel[View],
    updated_actor: Node,
    cognite_client: CogniteClient,
):
    yield [create_node(test_model.space, updated_actor, test_model)]
    cognite_client.data_modeling.instances.delete(
        nodes=(test_model.space, updated_actor.external_id)
    )


@pytest.fixture(scope="function")
def edge_list(
    test_model: DataModel[View],
    example_edge_actor_to_movie: Edge,
    example_edge_movie_to_actor: Edge,
    cognite_client: CogniteClient,
):
    edges = [
        create_edge(test_model.space, example_edge_actor_to_movie, test_model),
        create_edge(test_model.space, example_edge_movie_to_actor, test_model),
    ]
    yield edges
    cognite_client.data_modeling.instances.delete(
        edges=[
            (test_model.space, example_edge_actor_to_movie.external_id),
            (test_model.space, example_edge_movie_to_actor.external_id),
        ]
    )
    cognite_client.data_modeling.instances.delete(
        nodes=[(test_model.space, e.type.external_id) for e in edges]
    )


def node_info_dict(node: Node, space: str, type: str, version: int = 1) -> dict:
    return {
        "space": space,
        "instanceType": "node",
        "externalId": node.external_id,
        "version": version,
        **node.source_dict[type],
    }


@pytest.fixture(scope="function")
def instance_dataframes(
    example_actor: Node,
    example_movie: Node,
    test_model: DataModel[View],
) -> dict[str, pd.DataFrame]:
    # Expected per-view node frames (Actor/Person/Movie)
    actor_df = pd.DataFrame(
        node_info_dict(example_actor, test_model.space, "Actor"), index=[0]
    )
    person_df = pd.DataFrame(
        node_info_dict(example_actor, test_model.space, "Person"), index=[0]
    )
    movie_df = pd.DataFrame(
        node_info_dict(example_movie, test_model.space, "Movie"), index=[0]
    )
    return {
        "Actor": actor_df,
        "Person": person_df,
        "Movie": movie_df,
    }


@pytest.fixture(scope="function")
def expected_path_to_dataframe(
    instance_table_paths,
    instance_dataframes,
    test_model: DataModel[View],
    dm_replicator: DataModelingReplicator,
):
    """
    Return mapping of RAW node table paths → expected DataFrame for configured views.
    """
    cfg_views = dm_replicator.config.data_modeling[0].data_models[0].views or []
    model_xid = test_model.external_id
    model_ver = str(test_model.version)
    base = _raw_views_base(dm_replicator, test_model.space, model_xid, model_ver)

    mapping = {}
    for view_xid in cfg_views:
        path = f"{base}/{view_xid}/nodes"
        if view_xid in instance_dataframes:
            mapping[path] = instance_dataframes[view_xid]
    return mapping


# -------------------------
# Tests
# -------------------------

def test_data_model_sync_service_creation(
    dm_replicator,
    instance_table_paths,
    node_list,
    edge_list,
    expected_path_to_dataframe,
    cognite_client,
):
    apply_data_model_instances_in_cdf(node_list, edge_list, cognite_client)

    mv, selected = dm_replicator._get_data_model_views(
        dm_replicator.config.data_modeling[0],
        SimpleNamespace(external_id="Movie", version=None, views=dm_replicator.config.data_modeling[0].data_models[0].views),
    )
    assert mv is not None and len(selected) > 0, "Replicator didn't find any views on the Movie model"

    for _ in range(3):
        dm_replicator.process_spaces()
        time.sleep(1)

    assert_data_model_instances_in_s3(expected_path_to_dataframe, None)


def test_data_model_sync_service_update(
    dm_replicator,
    instance_table_paths,
    node_list,
    edge_list,
    updated_node_list,
    test_model: DataModel[View],
    cognite_client: CogniteClient,
):
    apply_data_model_instances_in_cdf(node_list, edge_list, cognite_client)
    for _ in range(2):
        dm_replicator.process_spaces()
        time.sleep(1)

    apply_data_model_instances_in_cdf(updated_node_list, [], cognite_client)
    for _ in range(3):
        dm_replicator.process_spaces()
        time.sleep(1)

    model_xid = test_model.external_id
    model_ver = str(test_model.version)
    base = _raw_views_base(dm_replicator, test_model.space, model_xid, model_ver)
    actor_nodes_path = f"{base}/Actor/nodes"

    dt = get_delta_table(None, actor_nodes_path)
    df = (
        dt.to_pandas()
          .drop(columns=DATA_MODEL_TIMESTAMP_COLUMNS, errors="ignore")
          .sort_index(axis=1)
          .reset_index(drop=True)
    )

    assert (df["externalId"] == "arnold_schwarzenegger").sum() >= 2
    arnold = df[df["externalId"] == "arnold_schwarzenegger"]

    assert set(arnold["version"].tolist()) >= {1, 2}

    v1_rows = arnold[arnold["version"] == 1]
    assert not v1_rows.empty
    assert not bool(v1_rows["wonOscar"].iloc[0])

    v2_rows = arnold[arnold["version"] == 2]
    assert not v2_rows.empty
    assert bool(v2_rows["wonOscar"].iloc[0])
