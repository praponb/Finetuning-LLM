WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
),
metrics AS (
    SELECT
        *,
        COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h,
        SUM(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_sum_15m,
        PERCENT_RANK() OVER (ORDER BY latency_ms ASC) AS latency_ms_percent_rank,
        COUNT(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_count_latency_ms
    FROM lines
    WHERE service IN ('search', 'checkout')
),
metrics_app_logs AS (
    SELECT
        id,
        service,
        log_ts,
        value,
        latency_ms_count_1h,
        latency_ms_sum_15m,
        latency_ms_percent_rank,
        attr_key_count_latency_ms,
        COUNT(attr_key_count_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS attr_key_count_latency_ms_count_30m,
        AVG(latency_ms_percent_rank) OVER w_part AS latency_ms_percent_rank_avg_1h,
        AVG(attr_key_count_latency_ms) OVER w_running AS attr_key_count_latency_ms_avg_1h,
        CUME_DIST() OVER w_grp AS latency_ms_percent_rank_cume_dist
    FROM metrics
    WINDOW w_part AS (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY latency_ms_percent_rank DESC NULLS LAST)
),
valid AS (
    SELECT *, ROUND(latency_ms_sum_15m, 1) AS latency_ms_sum_15m_rounded
    FROM metrics_app_logs
    WHERE log_ts >= TIMESTAMP '2025-06-01 14:00:00'
      AND latency_ms_percent_rank_avg_1h BETWEEN 0 AND 1
),
with_windows AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms_sum_15m,
        latency_ms_sum_15m_rounded,
        RANK() OVER (PARTITION BY service ORDER BY attr_key_count_latency_ms_count_30m DESC NULLS LAST) AS attr_key_count_latency_ms_count_30m_rank,
        MIN(attr_key_count_latency_ms) OVER (PARTITION BY service) AS service_min_attr_key_count_latency_ms
    FROM valid
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN latency_ms_sum_15m >= 11376 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_sum_15m_tier,
    CASE WHEN service_min_attr_key_count_latency_ms >= 16 THEN 'LARGE' ELSE 'SMALL' END AS service_min_attr_key_count_latency_ms_tier,
    ROUND(AVG(latency_ms_sum_15m_rounded), 2) AS avg_latency_ms_sum_15m_rounded,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count,
    COUNT(*) AS cnt,
    ROUND(SUM(latency_ms_sum_15m) / NULLIF(COUNT(CASE WHEN service = 'billing' THEN 1 END), 0), 2) AS latency_ms_sum_15m_per_billing,
    APPROX_PERCENTILE(latency_ms_sum_15m_rounded, 0.9) AS latency_ms_sum_15m_rounded_p90,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', log_ts), CASE WHEN latency_ms_sum_15m >= 11376 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN service_min_attr_key_count_latency_ms >= 16 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('HOUR', log_ts), CASE WHEN latency_ms_sum_15m >= 11376 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('HOUR', log_ts)))
HAVING checkout_rows >= 1
ORDER BY checkout_rows ASC;
