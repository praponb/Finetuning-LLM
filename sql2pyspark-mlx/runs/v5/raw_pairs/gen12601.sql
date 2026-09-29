WITH flattened AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
flattened_attrs AS (
    SELECT
        host,
        latency_ms,
        status_code,
        key,
        v,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE host != 'h4'
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_dense_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_percent_rank,
        AVG(is_flagged) OVER (PARTITION BY key) AS key_avg_is_flagged,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank
    FROM flattened_attrs
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY latency_ms_dense_rank DESC NULLS LAST) AS latency_ms_dense_rank_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY is_flagged DESC NULLS FIRST) AS is_flagged_percent_rank,
        CUME_DIST() OVER w_recent AS status_code_high_cume_dist
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY host ORDER BY status_code_high ASC NULLS FIRST)
),
valid AS (
    SELECT
        host,
        latency_ms,
        status_code,
        key,
        v,
        is_flagged,
        status_code_high,
        latency_ms_dense_rank,
        latency_ms_percent_rank,
        status_code_percent_rank,
        latency_ms_dense_rank_percent_rank,
        is_flagged_percent_rank,
        status_code_high_cume_dist,
        CAST(is_flagged_percent_rank AS INT) AS is_flagged_percent_rank_int,
        latency_ms_dense_rank_percent_rank + status_code_high_cume_dist AS latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist
    FROM enriched
    WHERE key IS NOT NULL
),
valid_app_logs AS (
    SELECT
        *,
        CEIL(is_flagged_percent_rank_int) AS is_flagged_percent_rank_int_whole,
        latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist / NULLIF(status_code_high_cume_dist, 0) AS latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist_per_status_code_high_cume_dist,
        COALESCE(key, 'N/A') AS key_clean
    FROM valid
    WHERE key IS NOT NULL
)
SELECT
    key,
    is_flagged,
    status_code_high,
    latency_ms_dense_rank,
    status_code_percent_rank,
    is_flagged_percent_rank,
    status_code_high_cume_dist,
    is_flagged_percent_rank_int,
    latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist
FROM valid_app_logs
ORDER BY latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist DESC
