WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(latency_ms) AS latency_ms_whole,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM exploded
    WHERE key IN ('region', 'version', 'trace')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value,
        latency_ms_whole,
        resolved_key,
        CUME_DIST() OVER (ORDER BY latency_ms_whole DESC NULLS LAST) AS latency_ms_whole_cume_dist,
        PERCENT_RANK() OVER (ORDER BY status_code) AS status_code_percent_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share
    FROM filtered
)
SELECT
    id,
    host,
    level,
    status_code,
    key,
    latency_ms_whole,
    resolved_key,
    latency_ms_whole_cume_dist,
    latency_ms_share
FROM with_windows
ORDER BY latency_ms_share DESC NULLS FIRST
