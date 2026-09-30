WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_value,
        LEAST(latency_ms, status_code) least_latency_ms_status_code
    FROM flattened
    WHERE level != 'WARN'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC) latency_ms_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY latency_ms DESC) latency_ms_percent_rank,
        CUME_DIST() OVER (ORDER BY latency_ms DESC NULLS FIRST) latency_ms_cume_dist,
        CUME_DIST() OVER (PARTITION BY level, service ORDER BY least_latency_ms_status_code DESC) least_latency_ms_status_code_cume_dist,
        SUM(latency_ms) OVER (PARTITION BY service) service_sum_latency_ms
    FROM cleaned
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(service) clean_service
    FROM metrics
    WHERE least_latency_ms_status_code > 69
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(least_latency_ms_status_code_cume_dist) OVER w_hist level_count_least_latency_ms_status_code_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY service, level ORDER BY latency_ms_cume_dist NULLS LAST) latency_ms_cume_dist_percent_rank,
        MIN(least_latency_ms_status_code_cume_dist) OVER w_hist level_min_least_latency_ms_status_code_cume_dist,
        PERCENT_RANK() OVER (ORDER BY latency_ms_dense_rank DESC NULLS LAST) latency_ms_dense_rank_percent_rank,
        AVG(least_latency_ms_status_code_cume_dist) OVER w_hist level_avg_least_latency_ms_status_code_cume_dist
    FROM valid
    WINDOW w_hist AS (PARTITION BY level)
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_value,
        least_latency_ms_status_code,
        latency_ms_dense_rank,
        latency_ms_percent_rank,
        latency_ms_cume_dist,
        least_latency_ms_status_code_cume_dist,
        service_sum_latency_ms,
        clean_service,
        level_count_least_latency_ms_status_code_cume_dist,
        latency_ms_cume_dist_percent_rank,
        level_min_least_latency_ms_status_code_cume_dist,
        latency_ms_dense_rank_percent_rank,
        level_avg_least_latency_ms_status_code_cume_dist,
        RANK() OVER (PARTITION BY level, service ORDER BY latency_ms_cume_dist) latency_ms_cume_dist_rank,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms_cume_dist_percent_rank DESC NULLS FIRST) latency_ms_cume_dist_percent_rank_rank
    FROM with_windows
)
SELECT
    id,
    service,
    level,
    latency_ms,
    attr_value,
    least_latency_ms_status_code,
    latency_ms_percent_rank,
    least_latency_ms_status_code_cume_dist,
    clean_service
FROM with_windows_app_logs
ORDER BY latency_ms DESC NULLS FIRST
