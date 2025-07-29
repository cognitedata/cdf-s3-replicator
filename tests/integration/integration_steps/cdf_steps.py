from datetime import datetime

from cognite.client import CogniteClient

from cognite.client.data_classes.data_modeling import Space, NodeApply, EdgeApply
from cognite.client.data_classes.data_modeling.ids import DataModelId

TIMESTAMP_COLUMN = "timestamp"

def create_data_model_in_cdf(
    test_space: Space, test_dml: str, cognite_client: CogniteClient
):
    # Create a data model in CDF
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
    return models.latest_version()


def apply_data_model_instances_in_cdf(
    node_list: list[NodeApply],
    edge_list: list[EdgeApply],
    cognite_client: CogniteClient,
):
    # Create data model instances in CDF
    return cognite_client.data_modeling.instances.apply(
        nodes=node_list, edges=edge_list
    )


def compare_timestamps(timestamp1: datetime, timestamp2: datetime) -> bool:
    return timestamp1.replace(microsecond=0) == timestamp2.replace(microsecond=0)


def delete_state_store_in_cdf(
    database: str,
    table: str,
    cognite_client: CogniteClient,
):
    all_rows = cognite_client.raw.rows.list(database, table)
    for row in all_rows:
        cognite_client.raw.rows.delete(database, table, row.key)
