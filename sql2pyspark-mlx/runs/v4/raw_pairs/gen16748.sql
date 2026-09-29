WITH exploded AS (
    SELECT
        id,
        log_ts,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
)
SELECT
    id,
    log_ts,
    status_code,
    attr_key,
    attr_value,
    LAG(status_code, 2) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) AS prev_status_code,
    NTILE(10) OVER (PARTITION BY attr_key ORDER BY status_code DESC, id, attr_key) AS status_code_bucket,
    AVG(status_code) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_code_avg_2,
    AVG(status_code) OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_code_avg_6
FROM exploded;
