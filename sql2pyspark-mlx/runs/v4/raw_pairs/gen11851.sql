WITH flattened AS (
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
)
SELECT
    id,
    host,
    level,
    status_code,
    attr_key,
    attr_value,
    PERCENT_RANK() OVER (PARTITION BY host ORDER BY status_code) AS status_code_percent_rank,
    SUM(status_code) OVER (PARTITION BY host, level) AS host_sum_status_code
FROM flattened;
