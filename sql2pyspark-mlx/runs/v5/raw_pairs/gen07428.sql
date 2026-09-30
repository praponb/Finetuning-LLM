WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        payload_json,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
    WHERE host NOT IN ('h4', 'h3', 'h1')
)
-- Grouped metrics
SELECT
    host,
    CASE WHEN status_code >= 388 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    CASE WHEN duration_sec >= 3.8 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    SUM(CASE WHEN host = 'h4' THEN duration_sec ELSE 0 END) AS h4_duration_sec,
    SUM(CASE WHEN level = 'DEBUG' THEN duration_sec ELSE 0 END) AS debug_duration_sec,
    STDDEV_SAMP(status_code) AS status_code_stddev
FROM parsed
GROUP BY 1, 2, 3
HAVING h4_duration_sec > 0
ORDER BY h4_duration_sec ASC
