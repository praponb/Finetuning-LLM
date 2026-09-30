WITH unpacked AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        context,
        context['version'] AS version,
        context['trace'] AS trace,
        SIZE(context) AS num_context,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    level,
    CASE WHEN flag_bit4 >= 1 THEN 'LARGE' ELSE 'SMALL' END AS flag_bit4_tier,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes
FROM unpacked
GROUP BY ROLLUP (DATE_TRUNC('HOUR', log_ts), level, CASE WHEN flag_bit4 >= 1 THEN 'LARGE' ELSE 'SMALL' END)
HAVING span_minutes < 1000000
ORDER BY span_minutes;
