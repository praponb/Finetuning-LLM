WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM lines
    WHERE status_code BETWEEN 393 AND 413
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY k ORDER BY is_flagged, id, k) AS is_flagged_bucket,
        LAG(status_code_high, 1) OVER (ORDER BY is_flagged, id, k) AS prev_status_code_high,
        CUME_DIST() OVER (ORDER BY status_code_high) AS status_code_high_cume_dist,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k), 4) AS status_code_share
    FROM flattened_attrs
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        k,
        status_code_high,
        is_flagged,
        is_flagged_bucket,
        prev_status_code_high,
        status_code_high_cume_dist,
        status_code_share,
        SUM(is_flagged) OVER w_ordered AS cumulative_is_flagged,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY is_flagged, id, k) AS event_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY k ORDER BY is_flagged_bucket, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        k,
        COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
        SUM(status_code) FILTER (WHERE k = 'version') AS version_total_status_code
    FROM with_windows
    GROUP BY k
)
SELECT
    k,
    version_count,
    version_total_status_code
FROM totals
ORDER BY k DESC NULLS LAST, version_count
LIMIT 5;
