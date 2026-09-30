WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
)
-- Window metrics over several different partitions
SELECT
    id,
    level,
    key,
    value,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY service, level), 4) AS status_code_share,
    DENSE_RANK() OVER (PARTITION BY key ORDER BY status_code DESC) AS status_code_dense_rank,
    SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_sum_15m,
    PERCENT_RANK() OVER (PARTITION BY level ORDER BY status_code ASC) AS status_code_percent_rank,
    MAX(status_code) OVER (PARTITION BY service) AS service_max_status_code
FROM exploded
