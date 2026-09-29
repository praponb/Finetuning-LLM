WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        level,
        status_code,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_dense_rank,
        AVG(has_fault) OVER w_recent AS host_avg_has_fault,
        MIN(has_fault) OVER w_recent AS host_min_has_fault
    FROM parsed
    WHERE level = 'INFO'
    WINDOW w_recent AS (PARTITION BY host)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        attr_value,
        has_fault,
        status_code_high_dense_rank,
        CUME_DIST() OVER (PARTITION BY level ORDER BY status_code_high NULLS FIRST) AS status_code_high_cume_dist,
        AVG(has_fault) OVER (PARTITION BY level) AS level_avg_has_fault,
        RANK() OVER (PARTITION BY host ORDER BY status_code DESC) AS status_code_rank,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY host) AS host_stddev_samp_status_code_high
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        attr_value,
        has_fault,
        status_code_high_cume_dist,
        level_avg_has_fault,
        status_code_rank,
        host_stddev_samp_status_code_high,
        RANK() OVER (ORDER BY status_code_high_cume_dist DESC) AS status_code_high_cume_dist_rank,
        DENSE_RANK() OVER w_ordered AS status_code_high_dense_rank_dense_rank
    FROM scored
    WINDOW w_ordered AS (ORDER BY status_code_high_dense_rank NULLS FIRST)
)
SELECT
    id,
    status_code,
    attr_value,
    has_fault,
    status_code_high_cume_dist,
    level_avg_has_fault,
    host_stddev_samp_status_code_high,
    status_code_high_cume_dist_rank,
    status_code_high_dense_rank_dense_rank
FROM metrics
ORDER BY status_code_high_cume_dist_rank NULLS FIRST
