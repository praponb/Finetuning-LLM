WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
)
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms NULLS LAST) AS latency_ms_percent_rank,
    MAX(latency_ms) OVER (PARTITION BY k) AS k_max_latency_ms,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share
FROM flattened;
