WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, value
)
SELECT
    id,
    host,
    log_ts,
    status_code,
    k,
    value,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share,
    AVG(status_code) OVER (ORDER BY log_ts, id, k ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_code_avg_5
FROM exploded
WHERE k IS NOT NULL;
