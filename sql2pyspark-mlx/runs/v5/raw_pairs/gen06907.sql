WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
)
-- Running and moving aggregates
SELECT
    service,
    latency_ms,
    v,
    CORR(latency_ms, status_code) OVER (PARTITION BY attr_key, service) corr_latency_ms_status_code,
    COUNT(status_code) OVER w_running status_code_count_30m,
    PERCENT_RANK() OVER (ORDER BY status_code DESC) status_code_percent_rank,
    MAX(status_code) OVER w_running status_code_max_30m
FROM flattened
WINDOW w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
