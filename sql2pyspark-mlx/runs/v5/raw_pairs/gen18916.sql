WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
scored AS (
    SELECT
        id,
        log_ts,
        COUNT(latency_ms) OVER w_recent AS latency_ms_count_2h,
        MAX(latency_ms) OVER w_recent AS latency_ms_max_2h,
        COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h
    FROM lines
    WHERE latency_ms <> 1548
    WINDOW w_recent AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT *, COALESCE(latency_ms_count_1h, 0) AS latency_ms_count_1h_filled
    FROM scored
    WHERE log_ts > TO_DATE('2025-06-01')
      AND TO_DATE(log_ts) >= '2025-07-01'
),
metrics AS (
    SELECT
        *,
        SUM(latency_ms_count_1h) OVER w_part AS latency_ms_count_1h_sum_1h,
        SUM(latency_ms_count_1h) OVER w_running AS latency_ms_count_1h_sum_2h,
        AVG(latency_ms_count_2h) OVER w_recent AS latency_ms_count_2h_avg_30m,
        COUNT(latency_ms_count_2h) OVER w_running AS latency_ms_count_2h_count_2h,
        MAX(latency_ms_max_2h) OVER w_recent AS latency_ms_max_2h_max_30m
    FROM cleaned
    WINDOW w_part AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
grouped AS (
    SELECT
        CASE WHEN latency_ms_count_1h_sum_1h >= 88 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_count_1h_sum_1h_tier,
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(log_ts) AS first_seen,
        COUNT(DISTINCT id) AS distinct_ids_2,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN latency_ms_count_1h_sum_1h >= 88 THEN 'HIGH' ELSE 'LOW' END ORDER BY (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 DESC) AS span_minutes_percent_rank
    FROM metrics
    GROUP BY latency_ms_count_1h_sum_1h_tier, hour_window
    HAVING (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 < 1000000
)
SELECT
    latency_ms_count_1h_sum_1h_tier,
    hour_window,
    distinct_ids,
    first_seen,
    distinct_ids_2,
    span_minutes,
    span_minutes_percent_rank
FROM grouped
ORDER BY hour_window DESC NULLS LAST
