WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    SIZE(COLLECT_SET(k)) AS num_ks,
    STDDEV_SAMP(status_code) AS status_code_stddev
FROM flattened
GROUP BY DATE_TRUNC('DAY', log_ts)
HAVING SIZE(COLLECT_SET(k)) >= 1
ORDER BY num_ks DESC;
