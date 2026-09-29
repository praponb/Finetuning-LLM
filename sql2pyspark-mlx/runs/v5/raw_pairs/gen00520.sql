WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        SIZE(context) AS num_context,
        context['trace'] AS trace,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        COALESCE(context['version'], 'unknown') AS version_or_default
    FROM app_logs
    WHERE status_code < 266
)
-- Running and moving aggregates
SELECT
    id,
    latency_ms,
    status_code,
    num_context,
    version_or_default,
    LAG(num_context) OVER (ORDER BY duration_sec, id) prev_num_context,
    LEAD(num_context) OVER (ORDER BY duration_sec, id) next_num_context,
    LEAD(latency_ms) OVER (ORDER BY duration_sec, id) next_latency_ms,
    ROW_NUMBER() OVER (ORDER BY duration_sec, id) seq_no,
    LAG(retry_count, 2) OVER (ORDER BY duration_sec, id) prev_retry_count
FROM flattened_attrs;
