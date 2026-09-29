WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, value
)
SELECT
    CASE WHEN status_code >= 396 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    MAX(log_ts) AS last_seen,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN service = 'billing' THEN 1 END), 0), 2) AS status_code_per_billing,
    AVG(status_code) AS avg_status_code_2,
    SUM(CASE WHEN service = 'billing' THEN status_code ELSE 0 END) AS billing_status_code
FROM flattened
GROUP BY 1
ORDER BY status_code_tier DESC NULLS LAST;
