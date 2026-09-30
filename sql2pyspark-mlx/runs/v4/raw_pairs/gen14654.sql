WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
)
SELECT
    *,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host, attr_key), 4) AS latency_ms_share,
    PERCENT_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_percent_rank
FROM exploded;
