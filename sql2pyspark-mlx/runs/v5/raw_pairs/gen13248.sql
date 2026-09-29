WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v,
        CASE WHEN level IN ('ERROR', 'INFO') THEN 'Core' ELSE 'Other' END AS level_group,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM flattened
    WHERE host IN ('h1', 'h4')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        host,
        level,
        latency_ms,
        level_group,
        CUME_DIST() OVER (PARTITION BY host, service ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist,
        STDDEV_SAMP(status_code) OVER (PARTITION BY level, service) AS level_stddev_samp_status_code,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code) AS status_code_percent_rank
    FROM cleaned
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        level,
        latency_ms,
        level_group,
        latency_ms_cume_dist,
        level_stddev_samp_status_code,
        status_code_percent_rank,
        LEAST(latency_ms_cume_dist, level_stddev_samp_status_code) AS least_latency_ms_cume_dist_level_stddev_samp_status_code,
        UPPER(host) AS host_upper
    FROM enriched
    WHERE latency_ms = 2136
),
scored AS (
    -- Ranking and gap features
    SELECT
        level,
        latency_ms,
        latency_ms_cume_dist,
        level_stddev_samp_status_code,
        status_code_percent_rank,
        least_latency_ms_cume_dist_level_stddev_samp_status_code,
        host_upper,
        CUME_DIST() OVER (ORDER BY least_latency_ms_cume_dist_level_stddev_samp_status_code DESC NULLS FIRST) AS least_latency_ms_cume_dist_level_stddev_samp_status_code_cume_dist,
        PERCENT_RANK() OVER (ORDER BY latency_ms NULLS FIRST) AS latency_ms_percent_rank,
        STDDEV_SAMP(least_latency_ms_cume_dist_level_stddev_samp_status_code) OVER w_ordered AS level_stddev_samp_least_latency_ms_cume_dist_level_stddev_samp_status_code,
        CUME_DIST() OVER (PARTITION BY level ORDER BY status_code_percent_rank NULLS FIRST) AS status_code_percent_rank_cume_dist
    FROM base
    WINDOW w_ordered AS (PARTITION BY level)
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_percent_rank_cume_dist >= 0.82 THEN 'HIGH' ELSE 'LOW' END AS status_code_percent_rank_cume_dist_tier,
    level,
    COUNT(DISTINCT level) AS distinct_levels,
    BIT_AND(latency_ms) AS latency_ms_bit_and
FROM scored
GROUP BY CUBE (CASE WHEN status_code_percent_rank_cume_dist >= 0.82 THEN 'HIGH' ELSE 'LOW' END, level);
