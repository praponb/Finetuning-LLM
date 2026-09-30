WITH unpacked AS (
    SELECT
        id,
        service,
        level,
        status_code,
        payload_json,
        context,
        SIZE(context) AS num_context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
    FROM app_logs
)
SELECT
    service,
    APPROX_PERCENTILE(retry_count, ARRAY(0.25, 0.5, 0.75)) AS retry_count_quartiles,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count
FROM unpacked
GROUP BY service
ORDER BY error_count DESC
