WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        log_ts,
        level,
        payload_json,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    level,
    COUNT(CASE WHEN tier = 'pro' THEN 1 END) AS pro_count,
    MIN(log_ts) AS first_seen,
    COUNT_IF(level = 'DEBUG') AS debug_events,
    PERCENTILE_APPROX(num_context, ARRAY(0.25, 0.5, 0.75)) AS num_context_quartiles,
    SUM(CASE WHEN level = 'INFO' THEN duration_sec ELSE 0 END) AS info_duration_sec
FROM unpacked
GROUP BY CASE WHEN duration_sec >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END, level
ORDER BY duration_sec_tier DESC NULLS FIRST;
