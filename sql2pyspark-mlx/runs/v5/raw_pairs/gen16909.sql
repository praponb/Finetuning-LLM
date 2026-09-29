WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        context['region'] AS region,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
)
-- Running and moving aggregates
SELECT
    id,
    level,
    latency_ms,
    has_trace,
    SUM(retry_count) OVER (PARTITION BY host ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_retry_count,
    ROW_NUMBER() OVER (PARTITION BY host, level ORDER BY latency_ms, id) row_seq,
    SUM(status_code_high) OVER (PARTITION BY level, host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_code_high
FROM flattened_attrs
