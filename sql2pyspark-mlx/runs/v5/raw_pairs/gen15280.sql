WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
)
-- Window metrics over several different partitions
SELECT
    id,
    host,
    latency_ms,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
    RANK() OVER (PARTITION BY level, service ORDER BY latency_ms) AS latency_ms_rank,
    PERCENT_RANK() OVER (PARTITION BY level, service ORDER BY status_code DESC) AS status_code_percent_rank,
    AVG(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_avg_2h,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share
FROM lines;
