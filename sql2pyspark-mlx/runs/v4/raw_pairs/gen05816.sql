WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
)
SELECT
    id,
    host,
    log_ts,
    status_code,
    key,
    v,
    RANK() OVER (PARTITION BY host ORDER BY status_code DESC) AS status_code_rank,
    MAX(status_code) OVER (PARTITION BY host) AS host_max_status_code,
    AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_avg_2h
FROM lines
