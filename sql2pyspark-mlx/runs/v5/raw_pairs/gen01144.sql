WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
)
SELECT
    CASE WHEN status_code >= 397 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    k,
    BIT_AND(status_code) AS status_code_bit_and,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN k = 'version' THEN 1 END), 0), 2) AS status_code_per_version
FROM exploded
GROUP BY CASE WHEN status_code >= 397 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', log_ts), k
HAVING ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN k = 'version' THEN 1 END), 0), 2) < 1000000
ORDER BY status_code_bit_and;
