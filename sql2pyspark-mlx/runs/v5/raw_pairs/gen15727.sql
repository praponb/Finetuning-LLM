WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        latency_ms,
        key,
        SUM(latency_ms) OVER (PARTITION BY key) AS key_sum_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms DESC) AS latency_ms_cume_dist
    FROM exploded
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        latency_ms / NULLIF(latency_ms_share, 0) AS latency_ms_per_latency_ms_share,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM metrics
    WHERE key IS NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        latency_ms,
        key,
        key_sum_latency_ms,
        latency_ms_cume_dist,
        key_filled,
        SUM(latency_ms_per_latency_ms_share) OVER (PARTITION BY level, key) AS level_sum_latency_ms_per_latency_ms_share
    FROM filtered
)
-- Grouped metrics
SELECT
    level,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS latency_ms_per_debug,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY level
HAVING error_count < 1000
ORDER BY distinct_ids DESC NULLS LAST;
