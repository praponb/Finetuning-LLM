WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        context,
        COALESCE(context['version'], 'none') AS version_or_default,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        context['trace'] AS trace
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MIN(log_ts) AS first_log_ts,
    MIN(latency_ms) AS min_latency_ms,
    COUNT(*) AS num_rows,
    MIN(log_ts) AS first_seen
FROM parsed
GROUP BY hour_window;
