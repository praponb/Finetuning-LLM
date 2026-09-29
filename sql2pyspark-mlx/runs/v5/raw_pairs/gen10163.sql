WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        level,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE host IN ('h1', 'h2', 'h4')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        level,
        status_code_high,
        is_flagged,
        COALESCE(status_code_high, 0) AS status_code_high_filled
    FROM unpacked
    WHERE status_code_high = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        host,
        level,
        status_code_high,
        is_flagged,
        status_code_high_filled,
        ROUND(status_code_high_filled, 1) AS status_code_high_filled_rounded,
        is_flagged - status_code_high AS is_flagged_minus_status_code_high
    FROM cleaned
    WHERE is_flagged < 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        host,
        status_code_high_filled_rounded,
        DENSE_RANK() OVER (ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_dense_rank,
        PERCENT_RANK() OVER (ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_percent_rank
    FROM base
)
SELECT is_flagged_dense_rank, is_flagged_percent_rank
FROM enriched;
