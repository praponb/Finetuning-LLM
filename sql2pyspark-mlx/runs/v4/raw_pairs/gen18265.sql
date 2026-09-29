WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        latency_ms,
        payload_json,
        context,
        COALESCE(context['version'], 'direct') AS version_or_default,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        context['region'] AS region,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
    FROM app_logs
    WHERE latency_ms BETWEEN 2017 AND 2205
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    APPROX_PERCENTILE(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    MIN(log_ts) AS first_log_ts
FROM parsed
GROUP BY 1, 2
ORDER BY hour_window DESC
