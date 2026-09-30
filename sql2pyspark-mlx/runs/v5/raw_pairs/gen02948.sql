WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        status_code,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
-- Grouped metrics
SELECT
    host,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    APPROX_PERCENTILE(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    MIN(log_ts) AS started_at,
    MIN(log_ts) AS first_log_ts,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN host = 'h3' THEN 1 END), 0), 2) AS status_code_per_h3,
    APPROX_PERCENTILE(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles_2
FROM unpacked
GROUP BY host, DATE_TRUNC('HOUR', log_ts)
ORDER BY span_minutes DESC;
