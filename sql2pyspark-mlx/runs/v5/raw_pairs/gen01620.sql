WITH parsed AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        context,
        context['trace'] AS trace,
        COALESCE(context['region'], 'none') AS region_or_default
    FROM app_logs
)
SELECT
    CASE WHEN status_code >= 438 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    host,
    SUM(status_code) AS total_status_code,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    APPROX_PERCENTILE(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles_2,
    MIN(log_ts) AS started_at
FROM parsed
GROUP BY 1, 2
HAVING SUM(status_code) >= 0
ORDER BY total_status_code DESC NULLS LAST
