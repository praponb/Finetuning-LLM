WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        level,
        latency_ms,
        status_code,
        key,
        value,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE key = 'trace'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        level,
        latency_ms,
        status_code,
        key,
        value,
        has_fault,
        CEIL(status_code) status_code_whole
    FROM unpacked
    WHERE status_code_high IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        latency_ms,
        key,
        value,
        has_fault,
        status_code_whole,
        has_fault + status_code_whole has_fault_plus_status_code_whole
    FROM filtered
    WHERE latency_ms < 1234
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        has_fault,
        has_fault_plus_status_code_whole,
        AVG(has_fault_plus_status_code_whole) OVER w_hist key_avg_has_fault_plus_status_code_whole,
        SUM(latency_ms) OVER w_hist key_sum_latency_ms,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY has_fault_plus_status_code_whole NULLS FIRST) has_fault_plus_status_code_whole_dense_rank
    FROM base
    WINDOW w_hist AS (PARTITION BY key)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_grp has_fault_plus_status_code_whole_dense_rank_2,
        DENSE_RANK() OVER w_recent key_sum_latency_ms_dense_rank
    FROM with_windows
    WINDOW w_grp AS (ORDER BY has_fault_plus_status_code_whole DESC NULLS FIRST), w_recent AS (ORDER BY key_sum_latency_ms DESC NULLS LAST)
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(key_sum_latency_ms_dense_rank, 0) key_sum_latency_ms_dense_rank_filled
    FROM scored
    WHERE has_fault_plus_status_code_whole_dense_rank_2 BETWEEN 3 AND 4
)
-- Ranking and gap features
SELECT
    has_fault_plus_status_code_whole_dense_rank,
    has_fault_plus_status_code_whole_dense_rank_2,
    key_sum_latency_ms_dense_rank_filled,
    PERCENT_RANK() OVER (ORDER BY has_fault_plus_status_code_whole_dense_rank_2) has_fault_plus_status_code_whole_dense_rank_2_percent_rank
FROM filtered_app_logs;
