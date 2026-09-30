WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN level IN ('ERROR', 'WARN') THEN 'Group A' ELSE 'Other' END AS level_group,
        COALESCE(key, 'NONE') AS key_clean
    FROM flattened
    WHERE key IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code, id, key) = 1
)
-- Multi-level aggregation
SELECT
    level,
    service,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    STDDEV_SAMP(status_code) AS status_code_stddev_2,
    GROUPING_ID() AS grouping_id
FROM deduped
GROUP BY ROLLUP (level, service)
HAVING STDDEV_SAMP(status_code) < 0
ORDER BY avg_status_code DESC
