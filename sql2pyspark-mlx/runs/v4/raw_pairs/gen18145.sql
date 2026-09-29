WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        latency_ms,
        payload_json,
        context,
        context['trace'] AS trace,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        context['version'] AS version,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms >= 2030 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_tier,
    host,
    APPROX_PERCENTILE(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    MAX(duration_sec) FILTER (WHERE host = 'h1') AS h1_max_duration_sec,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    MAX(latency_ms) AS max_latency_ms,
    SIZE(COLLECT_SET(host)) AS num_hosts_2,
    MAX(latency_ms) AS max_latency_ms_2
FROM unpacked
GROUP BY GROUPING SETS ((CASE WHEN latency_ms >= 2030 THEN 'HIGH' ELSE 'LOW' END, host), (CASE WHEN latency_ms >= 2030 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING num_hosts >= 1;
