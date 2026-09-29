WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        INITCAP(TRIM(region)) AS region_title,
        INITCAP(TRIM(store_name)) AS store_name_title,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date BETWEEN '2023-08-01' AND '2025-03-10'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(sqft, 1) OVER (PARTITION BY city ORDER BY sqft, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) AS row_seq,
        LAST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket
    FROM ranked
    WHERE UPPER(store_name) LIKE '%IAM%'
),
ranked_stores AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_bucket, id) AS dedup_rank_2
    FROM enriched
    QUALIFY dedup_rank_2 = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        dedup_rank,
        prev_sqft,
        sqft_bucket,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY dedup_rank, id) AS seq_no,
        LAG(prev_sqft, 1) OVER (ORDER BY dedup_rank_2, id) AS prev_prev_sqft,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY last_known_sqft DESC, id) AS last_known_sqft_bucket,
        AVG(prev_sqft) OVER (ORDER BY dedup_rank_2, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_sqft_avg_3_prior,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) AS sqft_share
    FROM ranked_stores
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        dedup_rank,
        sqft_bucket,
        prev_prev_sqft,
        sqft_share,
        AVG(prev_prev_sqft) OVER w_grp AS prev_prev_sqft_avg_3_prior,
        LAST_VALUE(sqft_bucket, TRUE) OVER w_grp_2 AS last_known_sqft_bucket_2,
        AVG(prev_prev_sqft) OVER w_recent AS prev_prev_sqft_avg_3_prior_2
    FROM metrics
    WINDOW w_grp AS (PARTITION BY city ORDER BY sqft_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_grp_2 AS (PARTITION BY city ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY city ORDER BY last_known_sqft_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    sqft_bucket,
    prev_prev_sqft,
    sqft_share,
    prev_prev_sqft_avg_3_prior,
    prev_prev_sqft_avg_3_prior_2
FROM with_windows;
