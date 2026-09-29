WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    host,
    log_ts,
    level,
    status_code,
    k,
    attr_value,
    MAX(status_code) OVER (PARTITION BY k, service) AS k_max_status_code,
    RANK() OVER (PARTITION BY service, level ORDER BY status_code DESC) AS status_code_rank
FROM exploded
