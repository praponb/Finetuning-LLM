WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        context['version'] AS version,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2023-09-01'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN latency_ms >= 1924 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    BIT_AND(status_code) AS status_code_bit_and
FROM parsed
GROUP BY DATE_TRUNC('HOUR', log_ts), CASE WHEN latency_ms >= 1924 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY hour_window
