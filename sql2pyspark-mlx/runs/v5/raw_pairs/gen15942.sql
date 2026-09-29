WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
)
SELECT
    id,
    service,
    host,
    attr_value,
    PERCENT_RANK() OVER w_ordered AS latency_ms_percent_rank,
    RANK() OVER (PARTITION BY service, key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank
FROM flattened
WINDOW w_ordered AS (PARTITION BY service ORDER BY latency_ms);
