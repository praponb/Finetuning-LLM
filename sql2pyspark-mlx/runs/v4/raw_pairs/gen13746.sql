WITH flattened_attrs AS (
    SELECT
        id,
        level,
        latency_ms,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        context['trace'] AS trace,
        context['region'] AS region,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
SELECT
    level,
    CASE WHEN latency_ms >= 2138 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN level = 'ERROR' THEN 1 END), 0), 2) AS latency_ms_per_error
FROM flattened_attrs
GROUP BY GROUPING SETS ((level, CASE WHEN latency_ms >= 2138 THEN 'PREMIUM' ELSE 'STANDARD' END), (level), ())
HAVING debug_count >= 1
