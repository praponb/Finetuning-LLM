WITH exploded AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
parsed AS (
    SELECT
        latency_ms,
        status_code,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
enriched AS (
    SELECT
        has_fault,
        status_code_high,
        CUME_DIST() OVER (ORDER BY status_code) status_code_cume_dist
    FROM parsed
    WHERE has_fault <= 1
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_code_high DESC) status_code_high_dense_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_high) status_code_high_percent_rank,
        RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) status_code_high_rank,
        DENSE_RANK() OVER (ORDER BY has_fault NULLS LAST) has_fault_dense_rank
    FROM enriched
),
cleaned AS (
    SELECT
        status_code_cume_dist,
        status_code_high_dense_rank,
        status_code_high_percent_rank,
        status_code_high_rank,
        has_fault_dense_rank,
        has_fault / NULLIF(status_code_cume_dist, 0) has_fault_per_status_code_cume_dist,
        status_code_high_rank - has_fault status_code_high_rank_minus_has_fault
    FROM scored
    WHERE status_code_high_percent_rank > 0.37
)
SELECT
    status_code_cume_dist,
    status_code_high_rank,
    has_fault_dense_rank,
    has_fault_per_status_code_cume_dist,
    CUME_DIST() OVER (ORDER BY status_code_high_rank_minus_has_fault DESC NULLS FIRST) status_code_high_rank_minus_has_fault_cume_dist,
    PERCENT_RANK() OVER (ORDER BY status_code_high_rank_minus_has_fault NULLS FIRST) status_code_high_rank_minus_has_fault_percent_rank,
    DENSE_RANK() OVER (ORDER BY status_code_high_percent_rank NULLS LAST) status_code_high_percent_rank_dense_rank
FROM cleaned
