WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
)
SELECT
    *,
    MAX(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h,
    MAX(status_code) OVER (PARTITION BY host) AS host_max_status_code,
    SUM(status_code) OVER (PARTITION BY host) AS host_sum_status_code,
    STDDEV_SAMP(status_code) OVER (PARTITION BY host) AS host_stddev_samp_status_code,
    CUME_DIST() OVER (PARTITION BY host ORDER BY status_code DESC) AS status_code_cume_dist
FROM flattened
