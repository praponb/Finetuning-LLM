WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
flattened_attrs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
enriched AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        has_fault,
        status_code_high,
        AVG(status_code) OVER (ORDER BY latency_ms, id, key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) status_code_avg_6_prior,
        CORR(latency_ms, status_code) OVER w_running corr_latency_ms_status_code,
        SUM(status_code_high) OVER (PARTITION BY host, service ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_code_high,
        DENSE_RANK() OVER (PARTITION BY service, key ORDER BY has_fault DESC NULLS LAST) has_fault_dense_rank
    FROM flattened_attrs
    WINDOW w_running AS (PARTITION BY key)
)
SELECT
    CASE WHEN has_fault >= 1 THEN 'HIGH' ELSE 'LOW' END AS has_fault_tier,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high,
    SUM(CASE WHEN key = 'trace' THEN has_fault_dense_rank ELSE 0 END) AS trace_has_fault_dense_rank,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(status_code_avg_6_prior, ARRAY(0.25, 0.5, 0.75)) AS status_code_avg_6_prior_quartiles
FROM enriched
GROUP BY CASE WHEN has_fault >= 1 THEN 'HIGH' ELSE 'LOW' END
HAVING cnt < 1
ORDER BY avg_status_code_high DESC NULLS FIRST
