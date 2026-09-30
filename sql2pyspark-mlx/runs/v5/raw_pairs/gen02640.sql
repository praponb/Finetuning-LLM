WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        payload_json,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        COALESCE(context['version'], 'direct') AS version_or_default
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN retry_count >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS retry_count_tier,
    service,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    MAX(flag_bit1) FILTER (WHERE service = 'checkout') AS checkout_max_flag_bit1,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes_2,
    MAX(log_ts) AS last_log_ts
FROM flattened_attrs
GROUP BY 1, 2
HAVING (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 > 0
ORDER BY checkout_max_flag_bit1 ASC NULLS FIRST;
