WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        status_code,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(status_code_high AS STRING) AS status_code_high_str
    FROM parsed
    WHERE status_code_high <= 1
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_code * 0.85, 2) AS status_code_adjusted,
        status_code_high - status_code AS status_code_high_minus_status_code
    FROM valid
    WHERE status_code_high <> 2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_code_adjusted DESC) AS status_code_adjusted_dense_rank,
        CUME_DIST() OVER (ORDER BY status_code_adjusted DESC NULLS LAST) AS status_code_adjusted_cume_dist,
        RANK() OVER (ORDER BY status_code_adjusted) AS status_code_adjusted_rank,
        CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist
    FROM valid_app_logs
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY is_flagged DESC) AS is_flagged_cume_dist,
        CUME_DIST() OVER w_part AS status_code_high_cume_dist
    FROM metrics
    WINDOW w_part AS (ORDER BY status_code_high DESC)
)
SELECT
    status_code,
    is_flagged,
    status_code_high,
    status_code_high_str,
    status_code_adjusted,
    status_code_high_minus_status_code,
    status_code_adjusted_dense_rank,
    status_code_adjusted_cume_dist,
    status_code_adjusted_rank
FROM metrics_app_logs
ORDER BY status_code_high DESC, status_code_adjusted_dense_rank;
