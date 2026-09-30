WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        level,
        status_code,
        key,
        value,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE status_code != 320
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY has_fault DESC NULLS LAST) has_fault_dense_rank,
        CUME_DIST() OVER (ORDER BY has_fault) has_fault_cume_dist,
        CUME_DIST() OVER (PARTITION BY key ORDER BY has_fault DESC NULLS FIRST) has_fault_cume_dist_2,
        DENSE_RANK() OVER (ORDER BY status_code DESC) status_code_dense_rank
    FROM flattened_attrs
    WHERE status_code_high = 2
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        level,
        status_code,
        key,
        has_fault,
        status_code_high,
        has_fault_dense_rank,
        has_fault_cume_dist,
        status_code_dense_rank,
        COALESCE(key, 'unassigned') key_filled,
        ROUND(has_fault_cume_dist / has_fault_dense_rank, 2) has_fault_cume_dist_per_has_fault_dense_rank
    FROM scored
    WHERE status_code_dense_rank <= 8
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        level,
        status_code,
        has_fault,
        status_code_high,
        has_fault_cume_dist,
        status_code_dense_rank,
        has_fault_cume_dist_per_has_fault_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY status_code_dense_rank) status_code_dense_rank_percent_rank,
        RANK() OVER (ORDER BY status_code DESC) status_code_rank,
        DENSE_RANK() OVER (PARTITION BY level, key ORDER BY status_code_high DESC NULLS FIRST) status_code_high_dense_rank,
        DENSE_RANK() OVER (ORDER BY status_code_dense_rank NULLS FIRST) status_code_dense_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY status_code_dense_rank DESC NULLS FIRST) status_code_dense_rank_dense_rank_2
    FROM filtered
),
report AS (
    SELECT
        level,
        has_fault,
        has_fault_cume_dist,
        has_fault_cume_dist_per_has_fault_dense_rank,
        status_code_dense_rank_percent_rank,
        status_code_rank,
        status_code_high_dense_rank,
        status_code_dense_rank_dense_rank,
        status_code_dense_rank_dense_rank_2
    FROM with_windows
    ORDER BY status_code_dense_rank_dense_rank DESC
)
SELECT
    level,
    has_fault,
    has_fault_cume_dist,
    status_code_rank,
    status_code_high_dense_rank,
    status_code_dense_rank_dense_rank,
    status_code_dense_rank_dense_rank_2
FROM report
ORDER BY status_code_dense_rank_dense_rank_2
