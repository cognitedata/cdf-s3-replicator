import os
import time

from deltalake import DeltaTable
import pandas as pd
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError

TIMESTAMP_COLUMN = "timestamp"
DATA_MODEL_TIMESTAMP_COLUMNS = ["lastUpdatedTime", "createdTime", "deletedTime"]
EVENT_CDF_COLUMNS = ["id", "createdTime", "lastUpdatedTime"]
EVENT_SORT_COLUMNS = "startTime"


def _s3_storage_options() -> dict:
    opts = {}
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if region:
        opts["AWS_REGION"] = region

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")

    if access_key and secret_key:
        opts["AWS_ACCESS_KEY_ID"] = access_key
        opts["AWS_SECRET_ACCESS_KEY"] = secret_key
    if session_token:
        opts["AWS_SESSION_TOKEN"] = session_token

    return opts


def s3_table_name(table_name: str) -> str:
    """
    Compose a Delta table URI under your S3 prefix.

    If S3_PREFIX is set (non-empty), we use it:
      s3://bucket/optional/prefix/Tables/<table_name>

    Otherwise, we fall back to AWS_S3_BUCKET:
      s3://<AWS_S3_BUCKET>/Tables/<table_name>
    """
    base = os.getenv("S3_PREFIX", "").strip()
    if base:
        return f"{base.rstrip('/')}/Tables/{table_name}"

    bucket = os.getenv("AWS_S3_BUCKET", "").strip()
    if not bucket:
        raise RuntimeError("Set S3_PREFIX (recommended) or AWS_S3_BUCKET in the environment.")
    return f"s3://{bucket}/Tables/{table_name}"


def get_delta_table(_credential, s3_table_path: str) -> DeltaTable:
    """
    Return a DeltaTable for a given S3 path, with a short retry so that
    freshly created tables have time to expose _delta_log.
    """
    opts = _s3_storage_options()

    last_err = None
    for attempt in range(6):
        try:
            return DeltaTable(s3_table_path, storage_options=opts)
        except TableNotFoundError as e:
            last_err = e
            if attempt < 5:
                time.sleep(1)
            else:
                raise


def delete_delta_table_data(_credential, path: str):
    """
    Delete all rows in the table (logical delete). Creates the table handle if it exists.
    `_credential` is ignored (compat).
    """
    try:
        delta_table = get_delta_table(_credential, path)
        delta_table.delete()
    except TableNotFoundError:
        print(f"Table not found {path}")


def read_deltalake_tables(tables_path: str, _credential):
    """
    Read a Delta table at `tables_path` into a pandas DataFrame.
    `_credential` is ignored (compat).
    """
    try:
        delta_table = get_delta_table(_credential, tables_path)
    except TableNotFoundError:
        print(f"Table not found {tables_path}, returning empty dataframe")
        return pd.DataFrame()
    df = delta_table.to_pandas()
    return df


def prepare_s3_dataframe_for_comparison(
    dataframe: pd.DataFrame, external_id: str
) -> pd.DataFrame:
    dataframe = dataframe.loc[dataframe["externalId"] == external_id]
    dataframe = dataframe.sort_values(by=[TIMESTAMP_COLUMN]).reset_index(drop=True)
    return dataframe


def write_tables_data_to_s3(
    _credential, data_frame: DataFrame, table_path: str
):
    """
    Append a pandas DataFrame to a Delta table at `table_path`.
    `_credential` is ignored (compat).
    """
    print(table_path)
    write_deltalake(
        table_path,
        data_frame,
        mode="append",
        storage_options=_s3_storage_options(),
    )
    return None


def remove_tables_data_from_s3(_credential, table_path: str):
    """
    Delete all data in the Delta table (logical delete).
    `_credential` is ignored (compat).
    """
    try:
        DeltaTable(
            table_uri=table_path,
            storage_options=_s3_storage_options(),
        ).delete()
    except Exception:
        pass


def prepare_test_dataframe_for_comparison(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.sort_values(by=[TIMESTAMP_COLUMN]).reset_index(drop=True)
    dataframe[TIMESTAMP_COLUMN] = pd.to_datetime(dataframe[TIMESTAMP_COLUMN])
    return dataframe


def assert_tables_data_in_s3(
    external_id: str,
    data_points: pd.DataFrame,
    tables_path: str,
    _credential,
):
    """
    Assert time series data matches exactly between expected DataFrame and S3 Delta.
    `_credential` is ignored (compat).
    """
    data_points_from_s3 = read_deltalake_tables(
        tables_path, _credential
    )
    s3_dataframe = prepare_s3_dataframe_for_comparison(
        data_points_from_s3, external_id
    )
    test_dataframe = prepare_test_dataframe_for_comparison(data_points)
    assert_frame_equal(test_dataframe, s3_dataframe, check_dtype=False)


def assert_data_model_instances_in_s3(
    path_to_expected: dict[str, pd.DataFrame],
    _credential,
):
    """
    Assert raw data-model tables (per-view + edges) match expected frames,
    ignoring created/lastUpdated/deleted timestamps.

    `path_to_expected` must be a mapping: { s3_uri -> expected_dataframe }.
    """
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.drop(columns=DATA_MODEL_TIMESTAMP_COLUMNS, errors="ignore")
              .sort_index(axis=1)
              .reset_index(drop=True)
        )

    for path, expected_df in path_to_expected.items():
        delta_table = get_delta_table(_credential, path)
        s3_dataframe = delta_table.to_pandas()

        expected_norm = _normalize(expected_df.copy())
        s3_norm = _normalize(s3_dataframe)

        assert_frame_equal(expected_norm, s3_norm, check_dtype=False)


def assert_data_model_instances_update(
    update_dataframe: tuple, _credential
):
    """
    Assert that updated nodes (e.g., version bumps) are reflected in S3 Delta.
    `_credential` is ignored (compat). `update_dataframe` is (path, expected_df).
    """
    path, expected_df = update_dataframe
    delta_table = get_delta_table(_credential, path)
    s3_dataframe = delta_table.to_pandas()

    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.drop(columns=DATA_MODEL_TIMESTAMP_COLUMNS, errors="ignore")
              .sort_index(axis=1)
              .reset_index(drop=True)
        )

    expected_norm = _normalize(expected_df.copy())
    s3_norm = _normalize(s3_dataframe)

    assert_frame_equal(expected_norm, s3_norm, check_dtype=False)
