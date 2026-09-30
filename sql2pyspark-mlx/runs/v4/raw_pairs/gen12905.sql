WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        context['trace'] AS trace
    FROM app_logs
)
-- Window metrics over several different partitions
SELECT
    id,
    host,
    latency_ms,
    status_code,
    context,
    status_code_high,
    trace,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
    STDDEV_SAMP(status_code_high) OVER (PARTITION BY host) AS host_stddev_samp_status_code_high
FROM parsed
