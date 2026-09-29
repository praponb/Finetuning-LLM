WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        level,
        status_code,
        context,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        COALESCE(context['region'], 'none') AS region_or_default
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    host,
    SUM(retry_count) AS total_retry_count,
    MAX(retry_count) FILTER (WHERE host = 'h1') AS h1_max_retry_count
FROM flattened_attrs
GROUP BY host
ORDER BY h1_max_retry_count DESC
