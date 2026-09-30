WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        v,
        ROUND(latency_ms * 1.07, 2) AS latency_ms_scaled,
        ROUND(latency_ms * 100, 2) AS latency_ms_scaled_2
    FROM lines
    WHERE log_ts > DATE '2025-06-01'
      AND latency_ms > 844
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(latency_ms AS DOUBLE) AS latency_ms_str,
        CASE WHEN latency_ms >= 2028 THEN 'High' WHEN latency_ms >= 1124 THEN 'Medium' ELSE 'Low' END AS latency_ms_band
    FROM cleaned
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 11:00:00')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        host,
        latency_ms,
        v,
        latency_ms_scaled,
        latency_ms_scaled_2,
        CUME_DIST() OVER (ORDER BY latency_ms_scaled_2 NULLS FIRST) AS latency_ms_scaled_2_cume_dist,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY latency_ms_scaled DESC) AS latency_ms_scaled_dense_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms_scaled NULLS FIRST) AS latency_ms_scaled_cume_dist,
        DENSE_RANK() OVER (ORDER BY latency_ms_scaled_2 NULLS FIRST) AS latency_ms_scaled_2_dense_rank,
        AVG(latency_ms_scaled) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_scaled_avg_30m
    FROM valid
)
SELECT
    host,
    latency_ms,
    v,
    latency_ms_scaled,
    latency_ms_scaled_2_cume_dist,
    latency_ms_scaled_dense_rank,
    latency_ms_scaled_cume_dist,
    latency_ms_scaled_avg_30m
FROM metrics
ORDER BY latency_ms_scaled DESC, latency_ms_scaled_avg_30m;
