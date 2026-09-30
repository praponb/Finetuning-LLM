WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        key,
        CUME_DIST() OVER w_part AS status_code_cume_dist,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share_2,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key, level), 4) AS status_code_share_3,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share_4
    FROM lines
    WINDOW w_part AS (PARTITION BY level ORDER BY status_code ASC)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_code_share_4 DESC NULLS LAST) AS status_code_share_4_dense_rank,
        STDDEV_SAMP(status_code_share_4) OVER (PARTITION BY key) AS key_stddev_samp_status_code_share_4
    FROM metrics
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(key_stddev_samp_status_code_share_4) AS key_stddev_samp_status_code_share_4_whole
    FROM enriched
    WHERE key = 'version'
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        status_code_cume_dist,
        status_code_share_2,
        status_code_share_3,
        key_stddev_samp_status_code_share_4,
        key_stddev_samp_status_code_share_4_whole,
        ROUND(status_code_share_4_dense_rank / SUM(status_code_share_4_dense_rank) OVER (PARTITION BY key), 4) AS status_code_share_4_dense_rank_share,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY status_code DESC) AS status_code_percent_rank
    FROM cleaned
    WHERE status_code_share_3 < 0.27
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN status_code_percent_rank >= 0.61 THEN 'High' WHEN status_code_percent_rank >= 0.35 THEN 'Medium' ELSE 'Low' END AS status_code_percent_rank_band,
        LENGTH(key) AS key_length
    FROM enriched_app_logs
    WHERE key IS NOT NULL
),
enriched_app_logs_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        status_code_share_3,
        STDDEV_SAMP(status_code_percent_rank) OVER w_part AS key_stddev_samp_status_code_percent_rank,
        STDDEV_SAMP(status_code_share_2) OVER w_part AS key_stddev_samp_status_code_share_2,
        COUNT(key_stddev_samp_status_code_share_4) OVER w_part AS key_count_key_stddev_samp_status_code_share_4,
        CUME_DIST() OVER (PARTITION BY key ORDER BY status_code_share_2 ASC) AS status_code_share_2_cume_dist
    FROM filtered
    WINDOW w_part AS (PARTITION BY key)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        key,
        CASE WHEN key_count_key_stddev_samp_status_code_share_4 >= 15 THEN 'HIGH' ELSE 'LOW' END AS key_count_key_stddev_samp_status_code_share_4_tier,
        CASE WHEN key_stddev_samp_status_code_percent_rank >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS key_stddev_samp_status_code_percent_rank_tier,
        AVG(key_stddev_samp_status_code_share_2) AS avg_key_stddev_samp_status_code_share_2,
        SIZE(COLLECT_SET(key)) AS num_keys,
        COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
        RANK() OVER (PARTITION BY key ORDER BY AVG(key_stddev_samp_status_code_share_2) DESC) AS avg_key_stddev_samp_status_code_share_2_rank
    FROM enriched_app_logs_2
    GROUP BY key, key_count_key_stddev_samp_status_code_share_4_tier, key_stddev_samp_status_code_percent_rank_tier
)
SELECT
    key,
    key_count_key_stddev_samp_status_code_share_4_tier,
    key_stddev_samp_status_code_percent_rank_tier,
    avg_key_stddev_samp_status_code_share_2,
    region_count
FROM summary;
