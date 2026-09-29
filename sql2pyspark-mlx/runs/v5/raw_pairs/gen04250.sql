WITH flattened AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
metrics AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST) latency_ms_percent_rank,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) latency_ms_avg_2h
    FROM flattened
),
cleaned AS (
    SELECT
        log_ts,
        key,
        latency_ms_avg_2h,
        latency_ms_percent_rank * latency_ms latency_ms_percent_rank_times_latency_ms
    FROM metrics
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 13:00:00')
),
enriched AS (
    SELECT
        log_ts,
        key,
        latency_ms_avg_2h,
        latency_ms_percent_rank_times_latency_ms,
        AVG(latency_ms_percent_rank_times_latency_ms) OVER w_ordered key_avg_latency_ms_percent_rank_times_latency_ms,
        AVG(latency_ms_avg_2h) OVER w_ordered key_avg_latency_ms_avg_2h,
        AVG(latency_ms_avg_2h) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) latency_ms_avg_2h_avg_1h
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY key)
),
valid AS (
    SELECT
        log_ts,
        latency_ms_avg_2h,
        latency_ms_percent_rank_times_latency_ms,
        key_avg_latency_ms_avg_2h,
        latency_ms_avg_2h_avg_1h,
        COALESCE(latency_ms_avg_2h, 0) latency_ms_avg_2h_filled,
        GREATEST(latency_ms_avg_2h, latency_ms_percent_rank_times_latency_ms) greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms
    FROM enriched
    WHERE key IS NOT NULL
      AND HOUR(log_ts) BETWEEN 9 AND 13
),
scored AS (
    SELECT *, SUM(greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms_sum_15m
    FROM valid
    WHERE greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms >= 67
)
SELECT
    latency_ms_avg_2h_avg_1h,
    latency_ms_avg_2h_filled,
    greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms,
    greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms_sum_15m
FROM scored
ORDER BY greatest_latency_ms_avg_2h_latency_ms_percent_rank_times_latency_ms_sum_15m, latency_ms_avg_2h_avg_1h DESC;
