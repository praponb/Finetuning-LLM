WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m,
        SUM(status_code) OVER w_running AS status_code_sum_2h
    FROM flattened
    WINDOW w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(attr_key, 1, 4) AS attr_key_prefix,
        SUBSTRING(host, 1, 4) AS host_prefix,
        COALESCE(attr_key, 'DIRECT') AS attr_key_clean
    FROM metrics
    WHERE log_ts > DATE '2025-06-01'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        CASE WHEN status_code_avg_30m >= 458 THEN 'HIGH' ELSE 'LOW' END AS status_code_avg_30m_tier,
        COUNT(*) AS cnt,
        MAX(log_ts) AS last_log_ts,
        COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count,
        MIN(log_ts) AS started_at,
        SUM(CASE WHEN service = 'auth' THEN status_code_avg_30m ELSE 0 END) AS auth_status_code_avg_30m,
        SORT_ARRAY(COLLECT_SET(host)) AS host_list,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', log_ts) ORDER BY SUM(CASE WHEN service = 'auth' THEN status_code_avg_30m ELSE 0 END) DESC) AS auth_status_code_avg_30m_percent_rank
    FROM base
    GROUP BY hour_window, status_code_avg_30m_tier
    HAVING h1_count > 1
)
SELECT
    hour_window,
    status_code_avg_30m_tier,
    cnt,
    last_log_ts,
    h1_count,
    started_at,
    host_list,
    auth_status_code_avg_30m_percent_rank
FROM totals
ORDER BY last_log_ts DESC
