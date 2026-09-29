WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level, service), 4) AS latency_ms_share,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER w_grp AS last_known_latency_ms,
        NTILE(10) OVER w_grp_2 AS latency_ms_bucket,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY latency_ms NULLS FIRST) AS latency_ms_percent_rank
    FROM lines
    WINDOW w_grp AS (ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY service ORDER BY latency_ms DESC, id, attr_key)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms_bucket, id, attr_key) AS event_seq,
        AVG(latency_ms_share) OVER (PARTITION BY attr_key ORDER BY latency_ms_percent_rank, id, attr_key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS latency_ms_share_avg_5,
        AVG(latency_ms_share) OVER (PARTITION BY attr_key) AS attr_key_avg_latency_ms_share,
        COUNT(latency_ms_bucket) OVER w_hist AS service_count_latency_ms_bucket
    FROM enriched
    WHERE service = 'checkout'
    WINDOW w_hist AS (PARTITION BY service)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms) AS latency_ms_percent_rank_2,
        ROW_NUMBER() OVER (PARTITION BY service, level ORDER BY latency_ms_bucket DESC, id, attr_key) AS event_seq_2,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_share_avg_5 DESC) AS latency_ms_share_avg_5_cume_dist
    FROM metrics
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        attr_key,
        service,
        event_seq_2
    FROM scored
) src
PIVOT (
    AVG(event_seq_2) AS amount,
    COUNT(1) AS n
    FOR service IN ('search', 'billing', 'checkout')
)
