WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        key,
        value,
        CASE WHEN key = 'version' THEN 1 ELSE 0 END AS is_version,
        COALESCE(key, 'N/A') AS key_filled
    FROM lines
    WHERE key IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        key,
        value,
        CONCAT_WS('-', level, key) AS level_key_key,
        COALESCE(key, 'N/A') AS key_filled_2
    FROM valid
    WHERE key IN ('region', 'version')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(level) AS clean_level
    FROM filtered
    WHERE key IS NOT DISTINCT FROM 'region'
      AND key IN ('version', 'region', 'trace')
)
-- Multi-level aggregation
SELECT
    key,
    level,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(key)) AS num_keys
FROM base
GROUP BY GROUPING SETS ((key, level), (key))
ORDER BY key DESC NULLS FIRST;
