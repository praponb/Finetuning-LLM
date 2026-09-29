WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        COALESCE(key, 'NONE') AS key_filled
    FROM exploded
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'trace'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_cume_dist,
        COUNT(status_code) OVER (PARTITION BY service, key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_count_15m,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_dense_rank
    FROM cleaned
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code_dense_rank) OVER (PARTITION BY service) AS service_avg_status_code_dense_rank,
        COUNT(latency_ms_cume_dist) OVER (PARTITION BY service, key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_cume_dist_count_1h,
        CUME_DIST() OVER (PARTITION BY service, key ORDER BY latency_ms NULLS LAST) AS latency_ms_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms_cume_dist NULLS LAST) AS latency_ms_cume_dist_dense_rank,
        MIN(days_since_log_ts) OVER (PARTITION BY service) AS service_min_days_since_log_ts
    FROM enriched
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        status_code,
        days_since_log_ts,
        key_filled,
        latency_ms_cume_dist,
        status_code_dense_rank,
        latency_ms_cume_dist_count_1h,
        latency_ms_cume_dist_2,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms_cume_dist_2 NULLS FIRST) AS latency_ms_cume_dist_2_rank
    FROM enriched_app_logs
    WHERE latency_ms_cume_dist_2 <= 0.3
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        days_since_log_ts,
        status_code_dense_rank,
        latency_ms_cume_dist_count_1h,
        latency_ms_cume_dist_2_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_dense_rank DESC NULLS LAST) AS status_code_dense_rank_percent_rank,
        SUM(latency_ms_cume_dist_2_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_cume_dist_2_rank_sum_30m
    FROM with_windows
)
SELECT status_code, status_code_dense_rank
FROM metrics
ORDER BY status_code_dense_rank DESC
