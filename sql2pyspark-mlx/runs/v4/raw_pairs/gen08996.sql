WITH flattened_attrs AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        payload_json,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        context['trace'] AS trace,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        COALESCE(context['version'], 'unknown') AS version_or_default,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
    WHERE latency_ms IS NOT NULL
)
SELECT
    *,
    FIRST_VALUE(latency_ms) IGNORE NULLS OVER (ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms,
    SUM(latency_ms) OVER (PARTITION BY host, level ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms
FROM flattened_attrs
