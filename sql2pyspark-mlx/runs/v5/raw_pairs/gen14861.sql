WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        context,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        context['trace'] AS trace
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN flag_bit4 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS flag_bit4_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(log_ts) AS first_log_ts
FROM flattened_attrs
GROUP BY flag_bit4_tier
