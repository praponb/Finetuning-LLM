WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_dense_rank,
        LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share
    FROM stores
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(sqft_dense_rank) AS sqft_dense_rank_whole
    FROM scored
    WHERE next_sqft < 9891
),
scored_stores AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_dense_rank,
        next_sqft,
        sqft_share,
        sqft_dense_rank_whole,
        AVG(sqft_dense_rank) OVER (PARTITION BY city ORDER BY sqft_share, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_dense_rank_avg_5_prior,
        NTILE(10) OVER (PARTITION BY region ORDER BY next_sqft, id) AS next_sqft_bucket,
        LEAD(next_sqft, 1) OVER (PARTITION BY city, region ORDER BY sqft_share, id) AS next_next_sqft
    FROM filtered
)
-- Grouped metrics
SELECT
    CASE WHEN next_next_sqft >= 16395 THEN 'HIGH' ELSE 'LOW' END AS next_next_sqft_tier,
    CASE WHEN sqft_dense_rank_avg_5_prior >= 9 THEN 'LARGE' ELSE 'SMALL' END AS sqft_dense_rank_avg_5_prior_tier,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    ROUND(AVG(sqft_dense_rank_whole), 2) AS avg_sqft_dense_rank_whole,
    RANK() OVER (PARTITION BY CASE WHEN next_next_sqft >= 16395 THEN 'HIGH' ELSE 'LOW' END ORDER BY STDDEV_SAMP(sqft) DESC) AS sqft_stddev_rank
FROM scored_stores
GROUP BY CASE WHEN next_next_sqft >= 16395 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN sqft_dense_rank_avg_5_prior >= 9 THEN 'LARGE' ELSE 'SMALL' END
