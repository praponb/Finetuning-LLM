WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        service,
        log_ts,
        latency_ms,
        k,
        v,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE latency_ms > 1349
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_flagged) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_flagged_avg_1h,
        SUM(latency_ms) OVER (PARTITION BY service, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_sum_30m
    FROM parsed
),
metrics AS (
    -- Ranking and gap features
    SELECT
        service,
        log_ts,
        latency_ms,
        k,
        v,
        is_flagged,
        status_code_high,
        is_flagged_avg_1h,
        latency_ms_sum_30m,
        DENSE_RANK() OVER w_running AS is_flagged_dense_rank,
        SUM(latency_ms_sum_30m) OVER w_grp AS latency_ms_sum_30m_sum_15m,
        MAX(status_code_high) OVER (PARTITION BY service) AS service_max_status_code_high,
        COUNT(is_flagged) OVER w_hist AS is_flagged_count_15m
    FROM scored
    WINDOW w_running AS (PARTITION BY k ORDER BY is_flagged DESC), w_grp AS (PARTITION BY service, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    service,
    CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(DISTINCT k) AS distinct_ks,
    PERCENTILE_APPROX(latency_ms, 0.95) AS latency_ms_p95
FROM metrics
GROUP BY service, CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', log_ts);
