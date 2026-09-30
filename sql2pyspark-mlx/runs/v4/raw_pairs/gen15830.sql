WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM flattened
    QUALIFY RANK() OVER (PARTITION BY level ORDER BY latency_ms DESC NULLS FIRST) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_ordered AS status_code_rank,
        AVG(latency_ms) OVER (PARTITION BY level ORDER BY latency_ms, id, attr_key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_2_prior,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS status_code_percent_rank
    FROM ranked
    WINDOW w_ordered AS (ORDER BY status_code DESC)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level, attr_key ORDER BY status_code_rank DESC) AS status_code_rank_dense_rank,
        RANK() OVER (ORDER BY latency_ms NULLS LAST) AS latency_ms_rank
    FROM with_windows
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        status_code_rank,
        latency_ms_avg_2_prior,
        status_code_percent_rank,
        status_code_rank_dense_rank,
        latency_ms_rank,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms_rank DESC) AS latency_ms_rank_dense_rank,
        LAST_VALUE(latency_ms_rank) IGNORE NULLS OVER (PARTITION BY level, attr_key ORDER BY latency_ms_rank, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms_rank,
        NTILE(3) OVER (PARTITION BY attr_key ORDER BY status_code_rank, id, attr_key) AS status_code_rank_bucket,
        LEAD(status_code_percent_rank, 2) OVER (ORDER BY latency_ms, id, attr_key) AS next_status_code_percent_rank,
        AVG(latency_ms_avg_2_prior) OVER (PARTITION BY attr_key) AS attr_key_avg_latency_ms_avg_2_prior
    FROM enriched
),
ranked_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms_avg_2_prior NULLS FIRST, id, attr_key) = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, host ORDER BY status_code DESC, id, attr_key) AS latest_rank
    FROM ranked_app_logs
    QUALIFY latest_rank = 1
)
-- Multi-level aggregation
SELECT
    attr_key,
    COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
    APPROX_PERCENTILE(latency_ms_avg_2_prior, 0.95) AS latency_ms_avg_2_prior_p95
FROM latest
GROUP BY attr_key
