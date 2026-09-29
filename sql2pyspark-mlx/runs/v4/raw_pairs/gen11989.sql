WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        COALESCE(context['version'], 'direct') AS version_or_default,
        context['region'] AS region
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code >= 403 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    BIT_OR(status_code) AS status_code_bit_or,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes_2
FROM unpacked
GROUP BY CASE WHEN status_code >= 403 THEN 'HIGH' ELSE 'LOW' END
HAVING duration_min < 0;
