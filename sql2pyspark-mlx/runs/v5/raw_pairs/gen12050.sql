WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value,
        UPPER(host) AS host_upper
    FROM exploded
    WHERE k IS DISTINCT FROM 'version'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, level
    FROM cleaned
    EXCEPT
    SELECT id, level
    FROM app_logs
    WHERE status_code >= 325
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.id = k.id AND s.level = k.level
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY k, level) AS k_avg_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level, k), 4) AS status_code_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share
    FROM reconciled
    WHERE status_code >= 357
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        level,
        SIZE(COLLECT_SET(k)) AS num_ks,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics
    GROUP BY hour_window, level
)
SELECT
    hour_window,
    level,
    num_ks,
    CASE WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS level_zone
FROM grouped
ORDER BY hour_window DESC NULLS LAST, num_ks DESC;
