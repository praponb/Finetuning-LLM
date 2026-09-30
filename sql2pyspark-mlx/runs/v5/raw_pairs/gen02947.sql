WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
)
SELECT
    key,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN status_code >= 438 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    COUNT(CASE WHEN service = 'auth' THEN 1 END) AS auth_count,
    APPROX_PERCENTILE(status_code, 0.5) AS status_code_p50,
    COUNT(*) AS cnt,
    MIN(log_ts) AS first_log_ts,
    COUNT(*) AS row_count
FROM exploded
GROUP BY ROLLUP (key, DATE_TRUNC('HOUR', log_ts), CASE WHEN status_code >= 438 THEN 'PREMIUM' ELSE 'STANDARD' END)
