WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        log_ts,
        latency_ms,
        CASE WHEN latency_ms >= 2000 THEN 'High' WHEN latency_ms >= 399 THEN 'Medium' ELSE 'Low' END latency_ms_level,
        ROUND(latency_ms, 2) latency_ms_rounded
    FROM flattened
    WHERE latency_ms BETWEEN 852 AND 1545
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), log_ts) days_since_log_ts,
        FLOOR(latency_ms_rounded) latency_ms_rounded_whole
    FROM filtered
    WHERE log_ts >= TIMESTAMP '2025-06-01 11:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY days_since_log_ts DESC) days_since_log_ts_percent_rank,
        MAX(latency_ms_rounded) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) latency_ms_rounded_max_2h,
        CUME_DIST() OVER (ORDER BY days_since_log_ts DESC NULLS FIRST) days_since_log_ts_cume_dist
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN latency_ms >= 1953 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    APPROX_PERCENTILE(latency_ms_rounded_max_2h, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_rounded_max_2h_quartiles,
    MIN(days_since_log_ts_cume_dist) AS min_days_since_log_ts_cume_dist,
    APPROX_PERCENTILE(days_since_log_ts, ARRAY(0.25, 0.5, 0.75)) AS days_since_log_ts_quartiles
FROM scored
GROUP BY 1
ORDER BY min_days_since_log_ts_cume_dist;
