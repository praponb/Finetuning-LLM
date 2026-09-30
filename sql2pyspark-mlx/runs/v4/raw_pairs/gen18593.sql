WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS status_code_rank,
        MAX(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m,
        DENSE_RANK() OVER w_ordered AS status_code_dense_rank,
        COUNT(status_code) OVER (PARTITION BY service, attr_key) AS service_count_status_code
    FROM exploded
    WINDOW w_ordered AS (ORDER BY status_code)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code_avg_15m) OVER w_ordered AS status_code_avg_15m_max_1h,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code_dense_rank DESC) AS status_code_dense_rank_dense_rank,
        SUM(service_count_status_code) OVER (PARTITION BY attr_key, service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS service_count_status_code_sum_15m
    FROM metrics
    WINDOW w_ordered AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        v,
        status_code_avg_15m,
        status_code_rank,
        status_code_max_30m,
        status_code_avg_15m_max_1h,
        status_code_dense_rank_dense_rank,
        service_count_status_code_sum_15m,
        ROUND(service_count_status_code_sum_15m * 1.1, 2) AS adjusted_service_count_status_code_sum_15m,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM enriched
    WHERE log_ts > TIMESTAMP '2025-06-01 06:00:00'
      AND attr_key IS NOT NULL
      AND service_count_status_code_sum_15m < 75
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        service,
        CASE WHEN status_code >= 425 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
        SUM(CASE WHEN service = 'checkout' THEN status_code_max_30m ELSE 0 END) AS checkout_status_code_max_30m,
        MAX(status_code) AS max_status_code,
        AVG(service_count_status_code_sum_15m) AS avg_service_count_status_code_sum_15m,
        MAX(log_ts) AS last_log_ts,
        SORT_ARRAY(COLLECT_SET(service)) AS service_list
    FROM cleaned
    GROUP BY DATE_TRUNC('HOUR', log_ts), service, CASE WHEN status_code >= 425 THEN 'LARGE' ELSE 'SMALL' END
    HAVING SUM(CASE WHEN service = 'checkout' THEN status_code_max_30m ELSE 0 END) >= 0
)
SELECT
    hour_window,
    service,
    status_code_tier,
    checkout_status_code_max_30m,
    max_status_code,
    last_log_ts,
    service_list
FROM stats
ORDER BY service
