WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY service ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms,
        AVG(latency_ms) OVER (ORDER BY log_ts, id, k ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_dense_rank,
        AVG(latency_ms) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_avg_3
    FROM lines
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        log_ts,
        level,
        k,
        attr_value,
        first_known_latency_ms,
        status_code_dense_rank,
        CAST(log_ts AS DATE) AS log_ts_day,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM enriched
    WHERE k IN ('trace', 'region', 'version')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Grouped metrics
SELECT
    level,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(DISTINCT k) AS distinct_ks,
    COUNT(DISTINCT level) AS distinct_levels,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    COUNT(*) AS event_count,
    DENSE_RANK() OVER (PARTITION BY level ORDER BY COUNT(CASE WHEN level = 'ERROR' THEN 1 END) DESC) AS error_count_dense_rank
FROM sessions_app_logs
GROUP BY level, DATE_TRUNC('DAY', log_ts)
ORDER BY level;
