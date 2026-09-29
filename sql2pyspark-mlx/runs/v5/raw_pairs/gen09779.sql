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
    key,
    CASE WHEN status_code >= 413 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    COUNT(*) AS row_count,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    RANK() OVER (PARTITION BY key ORDER BY STDDEV_SAMP(status_code) DESC) AS status_code_stddev_rank
FROM lines
GROUP BY key, CASE WHEN status_code >= 413 THEN 'HIGH' ELSE 'LOW' END
ORDER BY status_code_tier DESC;
