WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        value,
        AVG(status_code) OVER w_grp AS status_code_avg_1h,
        PERCENT_RANK() OVER w_recent AS status_code_percent_rank,
        MAX(status_code) OVER w_part AS service_max_status_code
    FROM lines
    WINDOW w_grp AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY status_code DESC), w_part AS (PARTITION BY service, key)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        value,
        status_code_avg_1h,
        status_code_percent_rank,
        service_max_status_code,
        ROUND(status_code / service_max_status_code, 2) AS status_code_per_service_max_status_code,
        COALESCE(key, 'N/A') AS resolved_key
    FROM scored
    WHERE status_code_avg_1h BETWEEN 263 AND 382
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY status_code_percent_rank) AS status_code_percent_rank_percent_rank,
        SUM(status_code_avg_1h) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_1h_sum_15m,
        STDDEV_SAMP(status_code_percent_rank) OVER (PARTITION BY service) AS service_stddev_samp_status_code_percent_rank
    FROM base
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_percent_rank_percent_rank >= 0.66 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_percent_rank_percent_rank_tier,
    COUNT_IF(service = 'checkout') AS checkout_events,
    COUNT(CASE WHEN service = 'auth' THEN 1 END) AS auth_count,
    AVG(status_code_per_service_max_status_code) AS avg_status_code_per_service_max_status_code,
    COUNT(DISTINCT service) AS distinct_services
FROM metrics
GROUP BY status_code_percent_rank_percent_rank_tier
HAVING avg_status_code_per_service_max_status_code > 0;
