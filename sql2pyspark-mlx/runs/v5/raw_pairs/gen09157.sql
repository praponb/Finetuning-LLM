WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, MAX(latency_ms) OVER w_running AS attr_key_max_latency_ms
    FROM exploded
    WHERE service = 'checkout'
    WINDOW w_running AS (PARTITION BY attr_key)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        service,
        latency_ms,
        v,
        RANK() OVER (ORDER BY attr_key_max_latency_ms) AS attr_key_max_latency_ms_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share
    FROM with_windows
),
matched_keys AS (
    -- Set difference on keys
    SELECT service
    FROM metrics
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE level IN ('DEBUG', 'WARN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.service = k.service
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY service ORDER BY attr_key_max_latency_ms_rank DESC NULLS FIRST) AS attr_key_max_latency_ms_rank_cume_dist,
        MIN(latency_ms) OVER w_ordered AS service_min_latency_ms,
        DENSE_RANK() OVER w_grp AS latency_ms_dense_rank
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY service), w_grp AS (ORDER BY latency_ms NULLS LAST)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT service
    FROM enriched
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE host NOT IN ('h3', 'h2', 'h4')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.service = k.service
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT service
    FROM verified
    EXCEPT DISTINCT
    SELECT service
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 8 AND 12
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN remaining_keys_2 k ON s.service = k.service
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    service,
    metric_name,
    reading
FROM (
    SELECT
        service,
        latency_ms_share,
        attr_key_max_latency_ms_rank_cume_dist
    FROM kept_rows
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (latency_ms_share, attr_key_max_latency_ms_rank_cume_dist)
) u;
