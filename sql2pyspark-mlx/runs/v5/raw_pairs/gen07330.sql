WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        payload_json,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        context['region'] AS region,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    service,
    CASE WHEN latency_ms >= 1907 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_tier,
    SUM(duration_sec) FILTER (WHERE service = 'checkout') AS checkout_total_duration_sec,
    SIZE(COLLECT_SET(service)) AS num_services,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM unpacked
GROUP BY service, CASE WHEN latency_ms >= 1907 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_latency_ms DESC
