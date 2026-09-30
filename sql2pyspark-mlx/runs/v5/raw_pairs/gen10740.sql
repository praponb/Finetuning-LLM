WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        country,
        CASE WHEN country = 'SG' THEN 1 ELSE 0 END is_sg
    FROM sessions
    WHERE country IS NOT NULL
      AND country = 'SG'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_sg DESC, id) seq_no,
        NTILE(3) OVER (ORDER BY is_sg, id) is_sg_bucket,
        AVG(is_sg) OVER (PARTITION BY customer_id ORDER BY is_sg, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) is_sg_avg_6,
        MAX(is_sg) OVER (PARTITION BY country, customer_id) country_max_is_sg
    FROM cleaned
),
with_windows_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        seq_no,
        DENSE_RANK() OVER (ORDER BY country_max_is_sg DESC NULLS LAST) country_max_is_sg_dense_rank,
        AVG(is_sg_bucket) OVER (PARTITION BY country ORDER BY is_sg_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) is_sg_bucket_avg_3_prior,
        NTILE(4) OVER (ORDER BY is_sg_avg_6 DESC, id) is_sg_avg_6_bucket
    FROM with_windows
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY country_max_is_sg_dense_rank DESC, id) country_max_is_sg_dense_rank_bucket,
        AVG(country_max_is_sg_dense_rank) OVER (ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) country_max_is_sg_dense_rank_avg_3_prior,
        SUM(seq_no) OVER (ORDER BY country_max_is_sg_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_seq_no
    FROM with_windows_sessions
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(country_max_is_sg_dense_rank_bucket AS DOUBLE) country_max_is_sg_dense_rank_bucket_str
    FROM enriched
    WHERE country_max_is_sg_dense_rank_bucket BETWEEN 4 AND 5
)
-- Grouped metrics
SELECT
    CASE WHEN country_max_is_sg_dense_rank_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS country_max_is_sg_dense_rank_bucket_tier,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    MIN(is_sg_bucket_avg_3_prior) AS min_is_sg_bucket_avg_3_prior,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
FROM filtered
GROUP BY ROLLUP (CASE WHEN country_max_is_sg_dense_rank_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END);
