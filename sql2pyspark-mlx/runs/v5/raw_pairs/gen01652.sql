WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        attr_key,
        LEAD(latency_ms, 1) OVER w_part AS next_latency_ms,
        AVG(status_code) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_avg_2h,
        NTILE(4) OVER w_part_2 AS status_code_bucket
    FROM exploded
    WINDOW w_part AS (ORDER BY latency_ms ASC, id ASC, attr_key ASC), w_part_2 AS (PARTITION BY host ORDER BY status_code ASC, id ASC, attr_key ASC)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code_avg_2h DESC, id ASC, attr_key ASC) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        next_latency_ms,
        status_code_avg_2h,
        status_code_bucket,
        MAX(next_latency_ms) OVER w_grp AS running_max_next_latency_ms,
        MIN(next_latency_ms) OVER w_grp AS running_min_next_latency_ms,
        COUNT(next_latency_ms) OVER w_grp AS running_count_next_latency_ms,
        AVG(next_latency_ms) OVER (PARTITION BY host ORDER BY next_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_latency_ms_avg_6_prior
    FROM deduped
    WINDOW w_grp AS (PARTITION BY host ORDER BY status_code_avg_2h ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        attr_key,
        next_latency_ms,
        status_code_avg_2h,
        status_code_bucket,
        running_max_next_latency_ms,
        running_count_next_latency_ms,
        ROUND(next_latency_ms_avg_6_prior, 0) AS next_latency_ms_avg_6_prior_rounded
    FROM scored
    WHERE attr_key IS NOT NULL
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_key,
        next_latency_ms,
        status_code_avg_2h,
        running_max_next_latency_ms,
        running_count_next_latency_ms,
        next_latency_ms_avg_6_prior_rounded,
        AVG(status_code_avg_2h) OVER (PARTITION BY attr_key ORDER BY running_count_next_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_avg_2h_avg_3_prior,
        FIRST_VALUE(next_latency_ms) IGNORE NULLS OVER (PARTITION BY attr_key ORDER BY next_latency_ms_avg_6_prior_rounded ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_latency_ms,
        FIRST_VALUE(running_max_next_latency_ms, TRUE) OVER (PARTITION BY attr_key ORDER BY status_code_avg_2h ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_next_latency_ms,
        AVG(status_code_avg_2h) OVER (ORDER BY status_code_bucket ASC, id ASC, attr_key ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_avg_2h_avg_2_prior
    FROM cleaned
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code_avg_2h_avg_3_prior DESC, id ASC, attr_key ASC) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_avg_2h_avg_2_prior >= 440 THEN 'LARGE' ELSE 'SMALL' END AS status_code_avg_2h_avg_2_prior_tier,
    CASE WHEN first_known_next_latency_ms >= 2515 THEN 'HIGH' ELSE 'LOW' END AS first_known_next_latency_ms_tier,
    COUNT(*) FILTER (WHERE attr_key = 'region') AS region_rows,
    ROUND(SUM(status_code_avg_2h) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS status_code_avg_2h_per_region,
    SUM(CASE WHEN attr_key = 'version' THEN running_count_next_latency_ms ELSE 0 END) AS version_running_count_next_latency_ms,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN status_code_avg_2h_avg_2_prior >= 440 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(SUM(status_code_avg_2h) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) DESC) AS status_code_avg_2h_per_region_dense_rank
FROM latest
GROUP BY 1, 2;
