import pandas as pd
import pytest
import sqlfluff
from sqlalchemy import create_engine

from distiller.feature_store import (
    FeatureGroupMetadata,
    build_sql_query_with_metadata,
    build_sql_query_with_single_entity,
)


@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    entity_df = pd.DataFrame({"entity_id": [1, 1, 2, 3], "event_timestamp": [4, 5, 5, 6], "labels": [0, 1, 0, 1]})
    feature_df = pd.DataFrame(
        {"entity_id": [1, 2, 3], "feature_timestamp": [3, 4, 5], "sample": ["q", "w", "e"], "numbers": [3, 6, 2]}
    )

    entity_df.to_sql("entity_df", con=engine, index=False)
    feature_df.to_sql("feature_group", con=engine, index=False)
    return engine


@pytest.fixture
def sqlite_engine_with_creation_timestamp():
    engine = create_engine("sqlite:///:memory:")
    entity_df = pd.DataFrame({"entity_id": [1, 1, 2, 3], "event_timestamp": [4, 5, 5, 6], "labels": [0, 1, 0, 1]})
    feature_df = pd.DataFrame(
        {
            "entity_id": [1, 1, 2, 3],
            "feature_timestamp": [3, 3, 4, 5],
            "sample": ["q", "w", "e", "r"],
            "numbers": [2, 3, 6, 2],
            "creation_timestamp": [1, 2, 1, 1],
        }
    )

    entity_df.to_sql("entity_df", con=engine, index=False)
    feature_df.to_sql("feature_group", con=engine, index=False)
    return engine


@pytest.fixture
def metadata_with_creation_timestamp():
    return FeatureGroupMetadata(
        table_name="feature_group", feature_names=["sample", "numbers"], feature_creation_column="creation_timestamp"
    )


@pytest.fixture
def metadata():
    return FeatureGroupMetadata(
        table_name="feature_group",
        feature_names=["sample", "numbers"],
    )


def test_default_parameters(sqlite_engine):
    query_output = build_sql_query_with_single_entity(sqlite_engine, "entity_df", ["feature_group"])
    query_linted = sqlfluff.fix(query_output, dialect="sqlite")
    output = pd.read_sql(query_linted, con=sqlite_engine)
    assert output.shape[0] == 4
    assert "entity_id" in output.columns
    assert "event_timestamp" in output.columns
    assert "labels" in output.columns
    assert "feature_group__sample" in output.columns
    assert "feature_group__numbers" in output.columns


def test_with_feature_selection(sqlite_engine):
    query_output = build_sql_query_with_single_entity(
        sqlite_engine, "entity_df", ["feature_group"], ["feature_group.sample"]
    )
    query_linted = sqlfluff.fix(query_output, dialect="sqlite")
    output = pd.read_sql(query_linted, con=sqlite_engine)
    assert output.shape[0] == 4
    assert "entity_id" in output.columns
    assert "event_timestamp" in output.columns
    assert "labels" in output.columns
    assert "feature_group__sample" in output.columns
    assert "feature_group__numbers" not in output.columns


def test_explicit_metadata(sqlite_engine, metadata):
    query = build_sql_query_with_metadata("entity_df", [metadata], entity_columns=["labels"])
    query_linted = sqlfluff.fix(query, dialect="sqlite")
    print(query_linted)
    output = pd.read_sql(query_linted, con=sqlite_engine)
    assert output.shape[0] == 4
    assert "entity_id" in output.columns
    assert "event_timestamp" in output.columns
    assert "labels" in output.columns
    assert "feature_group__sample" in output.columns
    assert "feature_group__numbers" in output.columns


def test_explicit_metadata_feature_subset(sqlite_engine, metadata):
    query = build_sql_query_with_metadata("entity_df", [metadata], ["feature_group.sample"])
    query_linted = sqlfluff.fix(query, dialect="sqlite")
    print(query_linted)
    output = pd.read_sql(query_linted, con=sqlite_engine)
    assert output.shape[0] == 4
    assert "entity_id" in output.columns
    assert "event_timestamp" in output.columns
    assert "labels" not in output.columns
    assert "feature_group__sample" in output.columns
    assert "feature_group__numbers" not in output.columns


def test_with_creation_timestamp(sqlite_engine_with_creation_timestamp, metadata_with_creation_timestamp):
    query = build_sql_query_with_metadata("entity_df", [metadata_with_creation_timestamp], ["feature_group.sample"])
    query_linted = sqlfluff.fix(query, dialect="sqlite")
    print(query_linted)
    output = pd.read_sql(query_linted, con=sqlite_engine_with_creation_timestamp)
    assert output.shape[0] == 4
    assert "entity_id" in output.columns
    assert "event_timestamp" in output.columns
    assert "labels" not in output.columns
    assert "feature_group__sample" in output.columns
    assert "feature_group__numbers" not in output.columns
    assert "q" not in output["feature_group__sample"]
