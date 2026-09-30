WITH flattened_attrs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        COALESCE(context['region'], 'direct') AS region_or_default,
        context['region'] AS region,
        context['trace'] AS trace,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
SELECT
    tier,
    host,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN service = 'auth' THEN 1 END), 0), 2) AS status_code_per_auth,
    COUNT_IF(host = 'h1') AS h1_events,
    MIN(log_ts) AS first_log_ts
FROM flattened_attrs
GROUP BY GROUPING SETS ((tier, host), (tier), ())
ORDER BY tier NULLS LAST;
