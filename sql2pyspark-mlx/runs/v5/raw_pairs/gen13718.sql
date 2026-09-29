WITH unpacked AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        payload_json,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        SIZE(context) AS num_context
    FROM app_logs
)
SELECT
    level,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MAX(log_ts) AS last_seen,
    SUM(num_context) AS total_num_context,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    GROUPING_ID() AS grouping_level
FROM unpacked
GROUP BY GROUPING SETS ((level, DATE_TRUNC('HOUR', log_ts)), (level))
