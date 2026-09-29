WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    level,
    latency_ms,
    attr_key,
    attr_value,
    RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms) AS latency_ms_rank,
    DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms) AS latency_ms_dense_rank,
    CUME_DIST() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC) AS latency_ms_cume_dist,
    RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank_2
FROM exploded;
