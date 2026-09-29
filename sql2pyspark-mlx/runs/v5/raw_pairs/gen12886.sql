WITH flattened AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
scored AS (
    SELECT
        *,
        RANK() OVER w_recent status_code_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) latency_ms_share,
        MAX(latency_ms) OVER (PARTITION BY level) level_max_latency_ms,
        CUME_DIST() OVER (ORDER BY latency_ms NULLS FIRST) latency_ms_cume_dist,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) latency_ms_share_2
    FROM flattened
    WINDOW w_recent AS (PARTITION BY key, level ORDER BY status_code DESC)
),
metrics AS (
    SELECT
        level,
        status_code,
        key,
        attr_value,
        status_code_rank,
        level_max_latency_ms,
        latency_ms_share_2,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_cume_dist) latency_ms_cume_dist_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code DESC) status_code_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY latency_ms_share_2 NULLS LAST) latency_ms_share_2_percent_rank
    FROM scored
),
scored_app_logs AS (
    SELECT
        *,
        STDDEV_SAMP(latency_ms_cume_dist_cume_dist) OVER (PARTITION BY level) level_stddev_samp_latency_ms_cume_dist_cume_dist,
        CUME_DIST() OVER (ORDER BY status_code) status_code_cume_dist,
        CUME_DIST() OVER (PARTITION BY key ORDER BY latency_ms_share_2_percent_rank NULLS FIRST) latency_ms_share_2_percent_rank_cume_dist
    FROM metrics
),
enriched AS (
    SELECT
        *,
        MAX(status_code_dense_rank) OVER (PARTITION BY level) level_max_status_code_dense_rank,
        RANK() OVER (PARTITION BY key ORDER BY status_code_rank DESC NULLS LAST) status_code_rank_rank,
        ROUND(status_code_dense_rank / SUM(status_code_dense_rank) OVER (PARTITION BY key), 4) status_code_dense_rank_share
    FROM scored_app_logs
    WHERE status_code_dense_rank != 5
)
SELECT
    level,
    status_code,
    key,
    status_code_dense_rank,
    latency_ms_share_2_percent_rank,
    level_stddev_samp_latency_ms_cume_dist_cume_dist,
    latency_ms_share_2_percent_rank_cume_dist,
    level_max_status_code_dense_rank,
    MIN(status_code_cume_dist) OVER (PARTITION BY key) key_min_status_code_cume_dist,
    PERCENT_RANK() OVER (PARTITION BY key ORDER BY status_code_dense_rank) status_code_dense_rank_percent_rank,
    RANK() OVER (PARTITION BY key ORDER BY latency_ms_share_2_percent_rank NULLS LAST) latency_ms_share_2_percent_rank_rank,
    PERCENT_RANK() OVER (PARTITION BY key, level ORDER BY status_code_cume_dist DESC) status_code_cume_dist_percent_rank,
    AVG(status_code_cume_dist) OVER (PARTITION BY key) key_avg_status_code_cume_dist
FROM enriched
