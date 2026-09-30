WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
)
SELECT
    id,
    latency_ms,
    attr_key,
    value,
    MIN(status_code) OVER w_recent AS running_min_status_code,
    MAX(status_code) OVER w_recent AS running_max_status_code,
    SUM(status_code) OVER w_recent AS running_sum_status_code,
    COUNT(status_code) OVER w_recent AS running_count_status_code,
    PERCENT_RANK() OVER (ORDER BY status_code) AS status_code_percent_rank
FROM lines
WINDOW w_recent AS (PARTITION BY host ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
