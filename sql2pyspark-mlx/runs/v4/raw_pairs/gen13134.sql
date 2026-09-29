WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
cleaned AS (
    SELECT
        *,
        LOWER(k) AS k_lower,
        COALESCE(latency_ms, 0) AS latency_ms_filled,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM exploded
    WHERE log_ts > TIMESTAMP '2025-06-01 11:00:00'
      AND k IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY k, host ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_dense_rank,
        MAX(latency_ms) OVER (PARTITION BY host, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_max_15m
    FROM cleaned
),
filtered AS (
    SELECT
        *,
        TRIM(host) AS clean_host,
        latency_ms_max_15m + latency_ms_filled AS latency_ms_max_15m_plus_latency_ms_filled
    FROM with_windows
    WHERE k IS NOT NULL
),
with_windows_app_logs AS (
    SELECT
        *,
        AVG(latency_ms_max_15m_plus_latency_ms_filled) OVER w_part AS k_avg_latency_ms_max_15m_plus_latency_ms_filled,
        PERCENT_RANK() OVER (PARTITION BY host, k ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_percent_rank
    FROM filtered
    WINDOW w_part AS (PARTITION BY k, host)
),
totals AS (
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        CASE WHEN latency_ms_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_dense_rank_tier,
        SUM(latency_ms_max_15m) FILTER (WHERE k = 'trace') AS trace_total_latency_ms_max_15m,
        ROUND(AVG(latency_ms_max_15m), 2) AS avg_latency_ms_max_15m
    FROM with_windows_app_logs
    GROUP BY DATE_TRUNC('HOUR', log_ts), CASE WHEN latency_ms_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END
    HAVING SUM(latency_ms_max_15m) FILTER (WHERE k = 'trace') < 0
)
SELECT
    hour_window,
    latency_ms_dense_rank_tier,
    avg_latency_ms_max_15m
FROM totals
ORDER BY latency_ms_dense_rank_tier NULLS LAST;
