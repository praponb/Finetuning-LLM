WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(latency_ms) OVER (PARTITION BY host) AS host_min_latency_ms,
        MAX(latency_ms) OVER (PARTITION BY service) AS service_max_latency_ms
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY host, attr_key ORDER BY service_max_latency_ms) AS service_max_latency_ms_percent_rank,
        ROUND(host_min_latency_ms / SUM(host_min_latency_ms) OVER (PARTITION BY service, attr_key), 4) AS host_min_latency_ms_share,
        CUME_DIST() OVER (PARTITION BY service ORDER BY service_max_latency_ms) AS service_max_latency_ms_cume_dist
    FROM with_windows
)
-- Grouped metrics
SELECT
    service,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT service) AS distinct_services,
    COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count
FROM metrics
GROUP BY service
ORDER BY distinct_ids DESC;
