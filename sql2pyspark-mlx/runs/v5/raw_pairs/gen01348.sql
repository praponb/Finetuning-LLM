WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        context['region'] AS region,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
-- Running and moving aggregates
SELECT
    *,
    AVG(latency_ms) OVER (PARTITION BY service ORDER BY log_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_latency_ms,
    COUNT(latency_ms) OVER (PARTITION BY service ORDER BY log_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_latency_ms,
    SUM(latency_ms) OVER (PARTITION BY service ORDER BY log_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_latency_ms,
    LEAD(latency_ms, 2) OVER (PARTITION BY host ORDER BY log_ts, id) next_latency_ms
FROM unpacked
