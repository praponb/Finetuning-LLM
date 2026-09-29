WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(service) AS clean_service
    FROM exploded
    WHERE log_ts > TO_DATE('2025-06-01')
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN level IN ('WARN', 'DEBUG') THEN 'Group A' ELSE 'Other' END AS level_group,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM valid
    WHERE key IS NOT NULL
      AND log_ts >= DATE '2025-06-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        level,
        key,
        value,
        clean_service,
        level_group,
        resolved_key,
        TRIM(service) AS clean_service_2,
        COALESCE(key, 'NONE') AS key_clean
    FROM valid_app_logs
    WHERE key = 'trace'
      AND key IS DISTINCT FROM 'region'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM base
    EXCEPT
    SELECT level, id
    FROM app_logs
    WHERE level IN ('DEBUG', 'INFO')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.id = k.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(key) AS key_lower,
        LOWER(host) AS host_lower,
        COALESCE(key, 'OTHER') AS resolved_key_2
    FROM verified
    WHERE key IS NOT NULL
      AND key IN ('trace', 'version')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(key) AS key_lower_2,
        COALESCE(key, 'NONE') AS resolved_key_3
    FROM cleaned
    WHERE key IS NOT NULL
),
daily AS (
    -- Multi-level aggregation
    SELECT
        key,
        level,
        SORT_ARRAY(COLLECT_SET(host)) AS host_list,
        COUNT(DISTINCT key) AS distinct_keys,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows,
        SORT_ARRAY(COLLECT_SET(level)) AS level_list,
        COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count
    FROM filtered
    GROUP BY key, level
)
SELECT
    key,
    level,
    host_list,
    distinct_keys,
    event_count,
    h2_rows,
    level_list,
    CASE WHEN level RLIKE '^(WARN|ERROR)$' THEN 'OTHER_ZONE' WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'CORE' ELSE 'GROUP_A' END AS level_zone
FROM daily
ORDER BY h2_rows DESC NULLS LAST
