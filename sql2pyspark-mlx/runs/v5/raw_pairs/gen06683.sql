WITH parsed AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        payload_json,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        COALESCE(context['region'], 'unknown') AS region_or_default
    FROM app_logs
    WHERE log_ts < '2024-01-02'
)
SELECT
    CASE WHEN duration_sec >= 3.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN host = 'h2' THEN duration_sec ELSE 0 END) AS h2_duration_sec,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM parsed
GROUP BY CASE WHEN duration_sec >= 3.3 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('HOUR', log_ts), DATE_TRUNC('DAY', log_ts)
HAVING num_rows > 1;
