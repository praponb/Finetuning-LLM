WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        log_ts,
        payload_json,
        COALESCE(context['version'], 'direct') AS version_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        COALESCE(context['trace'], 'direct') AS trace_or_default
    FROM app_logs
)
-- Grouped metrics
SELECT
    CASE WHEN duration_sec >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    SIZE(COLLECT_SET(service)) AS num_services
FROM unpacked
GROUP BY CASE WHEN duration_sec >= 4.2 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', log_ts)
ORDER BY num_services DESC;
