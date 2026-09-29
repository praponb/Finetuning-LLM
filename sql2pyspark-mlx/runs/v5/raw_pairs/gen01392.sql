WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        latency_ms,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM lines
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        latency_ms,
        status_code,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS status_code_dense_rank,
        DENSE_RANK() OVER (ORDER BY is_flagged NULLS LAST) AS is_flagged_dense_rank
    FROM flattened_attrs
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        status_code * status_code_dense_rank AS status_code_times_status_code_dense_rank,
        ROUND(latency_ms * 0.85, 2) AS latency_ms_adjusted
    FROM with_windows
    WHERE status_code_dense_rank >= 6
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank,
        RANK() OVER (ORDER BY status_code_dense_rank NULLS FIRST) AS status_code_dense_rank_rank,
        DENSE_RANK() OVER w_running AS status_code_dense_rank_2,
        RANK() OVER (ORDER BY is_flagged_dense_rank NULLS LAST) AS is_flagged_dense_rank_rank
    FROM filtered
    WHERE latency_ms = 1419
    WINDOW w_running AS (ORDER BY status_code DESC)
)
SELECT
    latency_ms,
    status_code,
    is_flagged_dense_rank,
    status_code_times_status_code_dense_rank,
    latency_ms_adjusted,
    status_code_dense_rank_rank,
    status_code_dense_rank_2,
    is_flagged_dense_rank_rank
FROM with_windows_app_logs
