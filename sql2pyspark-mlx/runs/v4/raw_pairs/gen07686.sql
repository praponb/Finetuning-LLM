WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(latency_ms) AS latency_ms_whole,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key
    FROM flattened
    WHERE latency_ms > 863
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS event_seq,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms_whole NULLS FIRST) AS latency_ms_whole_dense_rank,
        SUM(latency_ms_whole) OVER (PARTITION BY level, attr_key ORDER BY latency_ms_whole, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms_whole
    FROM base
    WINDOW w_recent AS (PARTITION BY level ORDER BY latency_ms, id, attr_key)
)
-- Multi-level aggregation
SELECT
    attr_key,
    PERCENTILE_APPROX(cumulative_latency_ms_whole, ARRAY(0.25, 0.5, 0.75)) AS cumulative_latency_ms_whole_quartiles,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    APPROX_PERCENTILE(latency_ms_whole, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_whole_quartiles
FROM scored
GROUP BY attr_key;
