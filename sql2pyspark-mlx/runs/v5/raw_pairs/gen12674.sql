WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
)
SELECT
    CASE WHEN status_code >= 427 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    MAX(status_code) FILTER (WHERE k = 'trace') AS trace_max_status_code,
    COUNT(*) AS event_count,
    AVG(status_code) FILTER (WHERE k = 'region') AS region_avg_status_code,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    COUNT(*) AS num_rows
FROM exploded
GROUP BY CASE WHEN status_code >= 427 THEN 'HIGH' ELSE 'LOW' END
HAVING AVG(status_code) FILTER (WHERE k = 'region') > 0;
