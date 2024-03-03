
WITH entity_dataframe AS (
    SELECT *,
        ${entity_timestamp_col} as entity_timestamp
        % for entity_column in entity_ids:
        , ${entity_column}
        % endfor
        % for feature_group_name, entity_column, _, _, _ in feature_table_entities:
        , (
            '${entity_column}' || 
            CAST(${entity_column} as VARCHAR) ||
            CAST(${entity_timestamp_col} as VARCHAR) 
        ) AS ${feature_group_name}__entity_row_unique_id
        % endfor
    FROM ${entity_table}
)
% for feature_group_name, feature_entity_column, feature_table, feature_timestamp_col, feature_creation_col in feature_table_entities:
,
${feature_group_name}__entity_dataframe AS (
    SELECT 
        ${feature_entity_column},
        entity_timestamp,
        ${feature_group_name}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY 
        ${feature_entity_column},
        entity_timestamp,
        ${feature_group_name}__entity_row_unique_id
)
,
${feature_group_name}__base AS (
    SELECT
        ${feature_timestamp_col} as event_timestamp
        % if feature_creation_col != "":
        , ${feature_creation_col} as creation_timestamp
        % else:
        , 1 as creation_timestamp
        % endif
        , entity_dataframe.${feature_group_name}__entity_row_unique_id
        , ${feature_table}.*
    FROM ${feature_table}
    INNER JOIN ${feature_group_name}__entity_dataframe as entity_dataframe
    ON ${feature_table}.${feature_entity_column} = entity_dataframe.${feature_entity_column}
    AND ${feature_table}.${feature_timestamp_col} <= entity_dataframe.entity_timestamp
)
,
/*
 Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
${feature_group_name}__cleaned AS (
    SELECT base.*,
    FROM ${feature_group_name}__base as base
    INNER JOIN 
    (
        select 
            ${feature_entity_column},
            ${feature_group_name}__entity_row_unique_id,
            max(event_timestamp) as max_event_timestamp, 
            max(creation_timestamp) as creation_timestamp
        from ${feature_group_name}__base
        group by 1, 2
    ) as latest
    ON 
        base.${feature_entity_column} = latest.${feature_entity_column}
        AND base.${feature_group_name}__entity_row_unique_id = latest.${feature_group_name}__entity_row_unique_id
        AND base.event_timestamp = latest.max_event_timestamp
        AND base.creation_timestamp = latest.creation_timestamp
)
% endfor
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
*/
SELECT 
    entity_dataframe.event_timestamp
% for column in entity_ids:
    , entity_dataframe.${column}
% endfor
% for column in entity_full_names:
    , entity_dataframe.${column}
% endfor
% for column in feature_full_names:
    , ${column}
% endfor
FROM entity_dataframe
% for feature_group_name, feature_entity_column, feature_table, feature_timestamp_col, feature_creation_col in feature_table_entities:
LEFT JOIN ${feature_group_name}__cleaned
    ON ${feature_group_name}__cleaned.${feature_group_name}__entity_row_unique_id = entity_dataframe.${feature_group_name}__entity_row_unique_id
    AND ${feature_group_name}__cleaned.${feature_entity_column} = entity_dataframe.${feature_entity_column}
% endfor
