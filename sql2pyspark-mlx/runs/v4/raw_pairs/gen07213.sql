WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        value,
        is_new_session,
        session_id,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        LENGTH(key) AS key_length,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM islands
    WHERE log_ts >= TIMESTAMP '2025-06-02 00:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        value,
        is_new_session,
        session_id,
        days_since_log_ts,
        key_length,
        key_clean,
        SUM(is_new_session) OVER (PARTITION BY service ORDER BY days_since_log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_session,
        NTILE(10) OVER (PARTITION BY service ORDER BY status_code, id, key) AS status_code_bucket
    FROM filtered
)
-- Multi-level aggregation
SELECT
    key,
    CASE WHEN key_length >= 60 THEN 'PREMIUM' ELSE 'STANDARD' END AS key_length_tier,
    COUNT(DISTINCT level) AS distinct_levels,
    APPROX_PERCENTILE(status_code_bucket, 0.5) AS status_code_bucket_p50,
    SUM(CASE WHEN key = 'region' THEN cumulative_is_new_session ELSE 0 END) AS region_cumulative_is_new_session,
    COUNT(DISTINCT key) AS distinct_keys,
    ROUND(SUM(cumulative_is_new_session) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS cumulative_is_new_session_per_trace
FROM enriched
GROUP BY GROUPING SETS ((key, CASE WHEN key_length >= 60 THEN 'PREMIUM' ELSE 'STANDARD' END), (key))
HAVING status_code_bucket_p50 < 0
