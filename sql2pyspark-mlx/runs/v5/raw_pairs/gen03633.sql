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
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        level,
        latency_ms,
        key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM exploded
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(is_flagged) OVER w_recent AS next_is_flagged,
        LEAD(latency_ms) OVER w_recent AS next_latency_ms,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM unpacked
    WINDOW w_recent AS (PARTITION BY level ORDER BY status_code_high ASC, id ASC, key ASC)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        status_code_high,
        row_seq,
        LAST_VALUE(key) IGNORE NULLS OVER (ORDER BY next_latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_key,
        CUME_DIST() OVER (PARTITION BY level, host ORDER BY next_is_flagged DESC NULLS LAST) AS next_is_flagged_cume_dist
    FROM with_windows
    WHERE host = 'h2'
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(row_seq) OVER (PARTITION BY key ORDER BY row_seq ASC, id ASC, key ASC) AS prev_row_seq,
        LEAD(next_is_flagged_cume_dist) OVER (PARTITION BY key ORDER BY row_seq ASC, id ASC, key ASC) AS next_next_is_flagged_cume_dist,
        LAG(next_is_flagged_cume_dist) OVER (PARTITION BY key ORDER BY row_seq ASC, id ASC, key ASC) AS prev_next_is_flagged_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY row_seq ASC, id ASC, key ASC) AS row_seq_2,
        NTILE(4) OVER (PARTITION BY key ORDER BY status_code_high DESC, id ASC, key ASC) AS status_code_high_bucket
    FROM metrics
)
-- Multi-level aggregation
SELECT
    key,
    CASE WHEN prev_row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS prev_row_seq_tier,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    SUM(next_next_is_flagged_cume_dist) FILTER (WHERE key = 'region') AS region_total_next_next_is_flagged_cume_dist,
    APPROX_PERCENTILE(next_is_flagged_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS next_is_flagged_cume_dist_quartiles,
    APPROX_PERCENTILE(next_is_flagged_cume_dist, 0.5) AS next_is_flagged_cume_dist_p50
FROM with_windows_app_logs
GROUP BY key, prev_row_seq_tier
HAVING region_count > 1
ORDER BY region_total_next_next_is_flagged_cume_dist DESC NULLS FIRST
