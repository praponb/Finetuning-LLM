WITH unpacked AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        payload_json,
        COALESCE(context['version'], 'none') AS version_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
    WHERE level IN ('ERROR', 'WARN')
)
SELECT
    CASE WHEN latency_ms >= 2614 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    PERCENTILE_APPROX(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles,
    COUNT_IF(level = 'WARN') AS warn_events,
    MAX(log_ts) AS ended_at
FROM unpacked
GROUP BY latency_ms_tier
ORDER BY latency_ms_tier;
