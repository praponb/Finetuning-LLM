WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
kept_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM lines
    EXCEPT DISTINCT
    SELECT level, id
    FROM app_logs
    WHERE latency_ms <> 1911
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.level = k.level AND s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN attr_key IN ('trace', 'version') THEN 'Core' ELSE 'Other' END AS attr_key_group,
        UPPER(host) AS host_upper
    FROM kept_rows
    WHERE attr_key IS NOT NULL
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(attr_key, 'Unknown') AS attr_key_filled,
        SUBSTRING(level, 1, 4) AS level_prefix
    FROM valid
    WHERE host = 'h4'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        v,
        attr_key_group,
        host_upper,
        attr_key_filled,
        level_prefix,
        CONCAT_WS('-', level, attr_key) AS level_attr_key_key,
        CONCAT(host, ' - ', attr_key) AS host_attr_key,
        COALESCE(attr_key, 'NONE') AS attr_key_filled_2
    FROM valid_app_logs
    WHERE attr_key IS NOT NULL
      AND host <> 'h4'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN host IN ('h3', 'h4') THEN 'Core' ELSE 'Other' END AS host_group
    FROM filtered
    WHERE host = 'h2'
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        v,
        attr_key_group,
        attr_key_filled,
        level_prefix,
        host_attr_key,
        attr_key_filled_2,
        host_group,
        TRIM(host) AS clean_host
    FROM cleaned
    WHERE host IN ('h2', 'h3')
)
SELECT
    id,
    host,
    v,
    attr_key_group,
    level_prefix,
    host_attr_key,
    clean_host
FROM filtered_app_logs
ORDER BY host NULLS FIRST, id;
