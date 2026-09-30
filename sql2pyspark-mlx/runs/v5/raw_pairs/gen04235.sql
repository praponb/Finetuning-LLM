WITH flattened_attrs AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        payload_json,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        COALESCE(context['version'], 'none') AS version_or_default,
        context['trace'] AS trace,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        COALESCE(context['trace'], 'direct') AS trace_or_default
    FROM app_logs
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(DISTINCT level) AS distinct_levels,
    COUNT(*) AS event_count,
    COUNT_IF(level = 'WARN') AS warn_events,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    COUNT_IF(tier = 'free') AS free_events
FROM flattened_attrs
GROUP BY DATE_TRUNC('DAY', log_ts), DATE_TRUNC('HOUR', log_ts);
