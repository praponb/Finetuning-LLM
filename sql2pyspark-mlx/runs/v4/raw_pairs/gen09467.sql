WITH lines AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
)
SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    key,
    value,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
    RANK() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank
FROM lines
