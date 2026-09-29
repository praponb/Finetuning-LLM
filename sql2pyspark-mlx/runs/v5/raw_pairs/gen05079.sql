WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(latency_ms) OVER (PARTITION BY host, level ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        status_code_cume_dist,
        AVG(cumulative_latency_ms) OVER w_grp AS cumulative_latency_ms_avg_2_prior,
        STDDEV_SAMP(status_code_cume_dist) OVER (PARTITION BY host) AS host_stddev_samp_status_code_cume_dist
    FROM enriched
    WINDOW w_grp AS (PARTITION BY level ORDER BY cumulative_latency_ms, id, attr_key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(host_stddev_samp_status_code_cume_dist) OVER w_ordered AS running_avg_host_stddev_samp_status_code_cume_dist,
        COUNT(host_stddev_samp_status_code_cume_dist) OVER w_ordered AS running_count_host_stddev_samp_status_code_cume_dist,
        SUM(host_stddev_samp_status_code_cume_dist) OVER w_ordered AS running_sum_host_stddev_samp_status_code_cume_dist,
        MIN(host_stddev_samp_status_code_cume_dist) OVER w_ordered AS running_min_host_stddev_samp_status_code_cume_dist
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY host ORDER BY host_stddev_samp_status_code_cume_dist, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        latency_ms,
        attr_key,
        status_code_cume_dist,
        host_stddev_samp_status_code_cume_dist,
        running_avg_host_stddev_samp_status_code_cume_dist,
        running_sum_host_stddev_samp_status_code_cume_dist,
        running_min_host_stddev_samp_status_code_cume_dist,
        CEIL(cumulative_latency_ms_avg_2_prior) AS cumulative_latency_ms_avg_2_prior_whole,
        COALESCE(attr_key, 'DIRECT') AS attr_key_filled
    FROM scored
    WHERE running_avg_host_stddev_samp_status_code_cume_dist > 0.61
)
SELECT
    id,
    latency_ms,
    attr_key,
    host_stddev_samp_status_code_cume_dist,
    running_avg_host_stddev_samp_status_code_cume_dist,
    running_sum_host_stddev_samp_status_code_cume_dist,
    running_min_host_stddev_samp_status_code_cume_dist,
    cumulative_latency_ms_avg_2_prior_whole,
    CASE WHEN attr_key RLIKE '^(trace|version)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS attr_key_zone
FROM valid
ORDER BY running_avg_host_stddev_samp_status_code_cume_dist DESC
