WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        level,
        status_code,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY has_fault NULLS FIRST) AS has_fault_percent_rank
    FROM unpacked
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        status_code_high_rank,
        has_fault_percent_rank,
        has_fault + status_code_high AS has_fault_plus_status_code_high,
        COALESCE(status_code_high_rank, 0) AS status_code_high_rank_filled
    FROM enriched
    WHERE status_code_high_rank BETWEEN 4 AND 8
),
base AS (
    -- Keep recent, valid rows
    SELECT *, has_fault_plus_status_code_high / NULLIF(status_code_high_rank_filled, 0) AS has_fault_plus_status_code_high_per_status_code_high_rank_filled
    FROM filtered
    WHERE level = 'WARN'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY has_fault_percent_rank DESC NULLS FIRST) AS has_fault_percent_rank_percent_rank,
        MAX(has_fault_plus_status_code_high_per_status_code_high_rank_filled) OVER (PARTITION BY level) AS level_max_has_fault_plus_status_code_high_per_status_code_high_rank_filled
    FROM base
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(has_fault_percent_rank_percent_rank AS INT) AS has_fault_percent_rank_percent_rank_int
    FROM scored
    WHERE status_code_high_rank_filled <= 65
)
SELECT
    id,
    has_fault_plus_status_code_high,
    status_code_high_rank_filled,
    has_fault_plus_status_code_high_per_status_code_high_rank_filled,
    has_fault_percent_rank_percent_rank,
    level_max_has_fault_plus_status_code_high_per_status_code_high_rank_filled,
    has_fault_percent_rank_percent_rank_int
FROM valid
ORDER BY has_fault_percent_rank_percent_rank_int DESC NULLS FIRST, has_fault_percent_rank_percent_rank;
