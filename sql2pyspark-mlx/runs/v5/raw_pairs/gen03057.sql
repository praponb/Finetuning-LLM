WITH exploded AS (
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
)
SELECT
    *,
    SUM(status_code) OVER (PARTITION BY service) AS service_sum_status_code,
    SUM(status_code) OVER (ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
    NTILE(5) OVER (ORDER BY status_code DESC, id, key) AS status_code_bucket
FROM exploded;
