WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        status_code,
        payload_json,
        SIZE(context) AS num_context,
        context['region'] AS region,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count,
    MIN(log_ts) AS first_seen,
    BIT_OR(status_code) AS status_code_bit_or,
    BIT_OR(status_code) AS status_code_bit_or_2
FROM parsed
GROUP BY DATE_TRUNC('HOUR', log_ts), DATE_TRUNC('DAY', log_ts)
