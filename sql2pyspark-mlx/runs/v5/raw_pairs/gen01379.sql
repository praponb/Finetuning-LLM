WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
)
SELECT
    *,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
    AVG(latency_ms) OVER (PARTITION BY level, service) AS level_avg_latency_ms,
    DENSE_RANK() OVER w_hist AS status_code_dense_rank,
    RANK() OVER w_hist AS status_code_rank
FROM flattened
WINDOW w_hist AS (ORDER BY status_code)
