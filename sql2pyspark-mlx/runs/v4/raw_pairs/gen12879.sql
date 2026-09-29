WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN status_code >= 397 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    BIT_AND(status_code) AS status_code_bit_and,
    MIN(log_ts) AS started_at,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    APPROX_PERCENTILE(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    MIN(log_ts) AS started_at_2
FROM flattened
GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', log_ts), CASE WHEN status_code >= 397 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('HOUR', log_ts)))
ORDER BY span_minutes DESC NULLS LAST;
