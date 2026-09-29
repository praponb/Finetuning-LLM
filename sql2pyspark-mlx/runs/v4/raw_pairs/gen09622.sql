WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        context['region'] AS region,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        context['version'] AS version,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
    FROM app_logs
)
-- Grouped metrics
SELECT
    level,
    MAX(log_ts) AS last_seen,
    ROUND(AVG(is_flagged), 2) AS avg_is_flagged,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS latency_ms_per_debug,
    APPROX_PERCENTILE(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    SUM(CASE WHEN level = 'DEBUG' THEN is_flagged ELSE 0 END) AS debug_is_flagged
FROM parsed
GROUP BY level
HAVING ROUND(AVG(is_flagged), 2) < 0
ORDER BY level NULLS FIRST
