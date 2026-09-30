WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
)
-- Window metrics over several different partitions
SELECT
    host,
    level,
    status_code,
    k,
    SUM(status_code) OVER (PARTITION BY host) AS host_sum_status_code
FROM lines;
