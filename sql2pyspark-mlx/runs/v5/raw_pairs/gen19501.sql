WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST, id ASC, key ASC) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(latency_ms) OVER (PARTITION BY service) AS service_min_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_5_prior
    FROM deduped
),
scored_app_logs AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        key,
        attr_value,
        service_min_latency_ms,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms ASC NULLS LAST) AS latency_ms_cume_dist,
        SUM(latency_ms_avg_5_prior) OVER (PARTITION BY service, key ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms_avg_5_prior,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share
    FROM scored
    WHERE key IN ('version', 'region')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        key,
        latency_ms_share,
        LAG(latency_ms) OVER w_part AS prev_latency_ms,
        LAG(service_min_latency_ms) OVER w_part AS prev_service_min_latency_ms,
        LEAD(service_min_latency_ms) OVER w_part AS next_service_min_latency_ms,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM scored_app_logs
    WINDOW w_part AS (PARTITION BY key ORDER BY service_min_latency_ms ASC, id ASC, key ASC)
),
scored_app_logs_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        key,
        latency_ms_share,
        seq_no,
        AVG(next_service_min_latency_ms) OVER (PARTITION BY key ORDER BY prev_latency_ms ASC, id ASC, key ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next_service_min_latency_ms_avg_2_prior,
        PERCENT_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_percent_rank,
        SUM(latency_ms) OVER (PARTITION BY key ORDER BY next_service_min_latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms,
        ROUND(next_service_min_latency_ms / SUM(next_service_min_latency_ms) OVER (PARTITION BY key), 4) AS next_service_min_latency_ms_share
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        latency_ms,
        key,
        seq_no,
        next_service_min_latency_ms_avg_2_prior,
        latency_ms_percent_rank,
        cumulative_latency_ms,
        next_service_min_latency_ms_share,
        ROUND(cumulative_latency_ms * 1.15, 2) AS cumulative_latency_ms_adjusted,
        latency_ms + seq_no AS latency_ms_plus_seq_no,
        COALESCE(key, 'NONE') AS key_clean
    FROM scored_app_logs_2
    WHERE key IS NOT NULL
)
-- Multi-level aggregation
SELECT
    seq_no,
    CASE WHEN cumulative_latency_ms >= 9018 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_latency_ms_tier,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    PERCENTILE_APPROX(next_service_min_latency_ms_share, ARRAY(0.25, 0.5, 0.75)) AS next_service_min_latency_ms_share_quartiles
FROM valid
GROUP BY seq_no, CASE WHEN cumulative_latency_ms >= 9018 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(*) FILTER (WHERE key = 'trace') >= 1
