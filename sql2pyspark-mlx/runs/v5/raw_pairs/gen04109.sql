WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(attr_key, 'unassigned') AS attr_key_filled,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled_2
    FROM lines
    WHERE attr_key IN ('version', 'region', 'trace')
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        attr_key,
        value,
        attr_key_filled,
        attr_key_filled_2,
        SUBSTRING(level, 1, 4) AS level_prefix
    FROM base
    WHERE attr_key IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', attr_key, level) AS attr_key_level_key,
        LENGTH(level) AS level_length,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled_3
    FROM base_app_logs
    WHERE attr_key IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY level_length DESC, id, attr_key) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY latest_rank DESC, id, attr_key) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(level_length) AS level_length_whole,
        COALESCE(attr_key, 'DIRECT') AS resolved_attr_key
    FROM latest
    WHERE attr_key IS DISTINCT FROM 'version'
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN level_length >= 68 THEN 'HIGH' ELSE 'LOW' END AS level_length_tier,
        attr_key,
        SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
        MIN(level_length) AS min_level_length,
        GROUPING_ID() AS grouping_id
    FROM filtered
    GROUP BY ROLLUP (CASE WHEN level_length >= 68 THEN 'HIGH' ELSE 'LOW' END, attr_key)
)
SELECT attr_key, attr_key_list
FROM daily
ORDER BY attr_key DESC NULLS FIRST;
