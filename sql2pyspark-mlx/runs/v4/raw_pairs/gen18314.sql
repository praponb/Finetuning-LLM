WITH lines AS (
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
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY latency_ms NULLS FIRST) AS latency_ms_cume_dist,
        AVG(latency_ms) OVER (PARTITION BY level) AS level_avg_latency_ms,
        CORR(latency_ms, status_code) OVER (PARTITION BY host) AS corr_latency_ms_status_code,
        COUNT(latency_ms) OVER (PARTITION BY level) AS level_count_latency_ms
    FROM lines
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v,
        latency_ms_cume_dist,
        level_avg_latency_ms,
        corr_latency_ms_status_code,
        level_count_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        ROUND(level_avg_latency_ms / SUM(level_avg_latency_ms) OVER (PARTITION BY host, attr_key), 4) AS level_avg_latency_ms_share,
        DENSE_RANK() OVER w_grp AS latency_ms_cume_dist_dense_rank,
        ROUND(level_avg_latency_ms / SUM(level_avg_latency_ms) OVER (PARTITION BY attr_key, level), 4) AS level_avg_latency_ms_share_2,
        STDDEV_SAMP(corr_latency_ms_status_code) OVER (PARTITION BY host) AS host_stddev_samp_corr_latency_ms_status_code
    FROM metrics
    WINDOW w_grp AS (ORDER BY latency_ms_cume_dist DESC NULLS FIRST)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        v,
        latency_ms_cume_dist,
        level_avg_latency_ms,
        corr_latency_ms_status_code,
        level_count_latency_ms,
        latency_ms_share,
        level_avg_latency_ms_share,
        latency_ms_cume_dist_dense_rank,
        level_avg_latency_ms_share_2,
        CAST(level_avg_latency_ms AS DOUBLE) AS level_avg_latency_ms_num
    FROM scored
    WHERE latency_ms_share > 0.32
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        latency_ms_cume_dist,
        level_avg_latency_ms,
        level_count_latency_ms,
        latency_ms_share,
        level_avg_latency_ms_share,
        level_avg_latency_ms_share_2,
        level_avg_latency_ms_num,
        latency_ms_cume_dist_dense_rank / NULLIF(level_avg_latency_ms_share, 0) AS latency_ms_cume_dist_dense_rank_per_level_avg_latency_ms_share,
        CAST(level_count_latency_ms AS STRING) AS level_count_latency_ms_num
    FROM cleaned
    WHERE attr_key IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY level_avg_latency_ms_num NULLS FIRST) AS level_avg_latency_ms_num_dense_rank,
        PERCENT_RANK() OVER w_ordered AS status_code_percent_rank
    FROM base
    WINDOW w_ordered AS (PARTITION BY host ORDER BY status_code)
)
-- Grouped metrics
SELECT
    host,
    COUNT(*) AS num_rows,
    BIT_AND(status_code) AS status_code_bit_and,
    COUNT_IF(host = 'h1') AS h1_events,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list
FROM enriched
GROUP BY 1
HAVING h1_events >= 1
ORDER BY h1_events;
