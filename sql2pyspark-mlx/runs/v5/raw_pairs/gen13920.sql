WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
)
SELECT
    CASE WHEN status_code >= 462 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    service,
    level,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    APPROX_PERCENTILE(latency_ms, 0.9) AS latency_ms_p90,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT key) AS distinct_keys,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN status_code >= 462 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY BIT_OR(latency_ms) DESC) AS latency_ms_bit_or_percent_rank
FROM exploded
GROUP BY 1, 2, 3;
