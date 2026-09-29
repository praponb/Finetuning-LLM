WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        status_code,
        k,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE status_code > 324
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        status_code,
        k,
        is_flagged,
        status_code_high,
        ROW_NUMBER() OVER (ORDER BY is_flagged DESC, id, k) AS row_seq,
        SUM(status_code) OVER w_part AS cumulative_status_code,
        NTILE(3) OVER (ORDER BY is_flagged, id, k) AS is_flagged_bucket,
        SUM(status_code) OVER (ORDER BY is_flagged, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_2
    FROM unpacked
    WINDOW w_part AS (PARTITION BY k ORDER BY status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(is_flagged_bucket, TRUE) OVER (PARTITION BY k, service ORDER BY status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_flagged_bucket,
        LEAD(status_code_high, 2) OVER (PARTITION BY k ORDER BY cumulative_status_code, id, k) AS next_status_code_high,
        ROUND(cumulative_status_code_2 / SUM(cumulative_status_code_2) OVER (PARTITION BY service), 4) AS cumulative_status_code_2_share
    FROM enriched
)
SELECT
    id,
    k,
    status_code_high,
    row_seq,
    first_known_is_flagged_bucket,
    next_status_code_high,
    cumulative_status_code_2_share
FROM enriched_app_logs
ORDER BY k DESC NULLS FIRST
