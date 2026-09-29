WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        payload_json,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        context['version'] AS version,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms >= 2664 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    AVG(status_code) FILTER (WHERE host = 'h3') AS h3_avg_status_code,
    AVG(latency_ms) AS avg_latency_ms,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows,
    MAX(latency_ms) FILTER (WHERE host = 'h2') AS h2_max_latency_ms,
    APPROX_PERCENTILE(latency_ms, 0.9) AS latency_ms_p90
FROM parsed
GROUP BY CASE WHEN latency_ms >= 2664 THEN 'LARGE' ELSE 'SMALL' END
