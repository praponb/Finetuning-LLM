WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
)
SELECT
    *,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share_2,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share_3,
    CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist
FROM flattened
