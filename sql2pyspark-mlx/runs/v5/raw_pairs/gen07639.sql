WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
)
-- Multi-level aggregation
SELECT
    host,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    MIN(status_code) AS min_status_code,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(host = 'h2') AS h2_events,
    SUM(CASE WHEN host = 'h3' THEN latency_ms ELSE 0 END) AS h3_latency_ms
FROM lines
GROUP BY host;
