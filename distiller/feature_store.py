from typing import List, Optional
from pydantic import BaseModel, model_validator
from distiller.template_utils import TemplateUtils
from sqlalchemy import Table, MetaData


class FeatureGroupMetadata(BaseModel):
    table_name: str
    feature_names: List[str]
    feature_group_name: Optional[str] = None
    entity_id_column: str = "entity_id"
    feature_event_timestamp_column: str = "feature_timestamp"
    feature_creation_column: Optional[str] = None

    @model_validator(mode="after")
    def check_passwords_match(self) -> "FeatureGroupMetadata":
        self.feature_group_name = self.feature_group_name if self.feature_group_name is not None else self.table_name
        self.feature_creation_column = "" if self.feature_creation_column is None else self.feature_creation_column
        return self


def build_sql_query_with_metadata(
    entity_table: str,
    feature_group_metadata: List[FeatureGroupMetadata],
    features: List[str] = [],
    entity_columns: List[str] = [],
    entity_id_column="entity_id",
    entity_event_timestamp_column="event_timestamp",
):
    feature_table_entities = []
    entity_ids = []

    all_feature_group_features = []
    for metadata in feature_group_metadata:
        feature_table_entities.append(
            (
                metadata.feature_group_name,
                metadata.entity_id_column,
                metadata.table_name,
                metadata.feature_event_timestamp_column,
                metadata.feature_creation_column,
            )
        )
        entity_ids.append(metadata.entity_id_column)
        for feature in metadata.feature_names:
            all_feature_group_features.append(f"{metadata.feature_group_name}.{feature}")

    feature_full_names = []
    features = all_feature_group_features if len(features) == 0 else features
    for feature in features:
        feature_group, feature = feature.split(".")
        feature_full_names.append(f"{feature_group}__cleaned.{feature} as {feature_group}__{feature}")

    entity_ids = sorted(set(entity_ids))
    entity_full_names = [
        column for column in entity_columns if column not in [entity_id_column, entity_event_timestamp_column]
    ]
    return TemplateUtils.render_template(
        "offline_feature_retrieval.mako.sql",
        entity_table=entity_table,
        entity_column=entity_id_column,
        entity_timestamp_col=entity_event_timestamp_column,
        feature_table_entities=feature_table_entities,
        entity_ids=entity_ids,
        entity_full_names=entity_full_names,
        feature_full_names=feature_full_names,
    )


def build_sql_query_with_single_entity(
    engine,
    entity_table: str,
    feature_tables: List[str],
    features: List[str] = [],
    entity_id_column="entity_id",
    entity_event_timestamp_column="event_timestamp",
):
    entity_metadata = Table(entity_table, MetaData(), autoload_with=engine)
    entity_columns = [c.name for c in entity_metadata.columns]

    feature_group_metadata = []
    for feature_table in feature_tables:
        feature_table_info = Table(feature_table, MetaData(), autoload_with=engine)
        columns = [c.name for c in feature_table_info.columns]
        feature_group_metadata.append(FeatureGroupMetadata(table_name=feature_table, feature_names=columns))
    return build_sql_query_with_metadata(
        entity_table=entity_table,
        feature_group_metadata=feature_group_metadata,
        features=features,
        entity_columns=entity_columns,
        entity_id_column=entity_id_column,
        entity_event_timestamp_column=entity_event_timestamp_column,
    )
