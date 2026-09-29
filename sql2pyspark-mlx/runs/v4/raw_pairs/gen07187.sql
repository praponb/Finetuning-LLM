WITH ranked AS (
    SELECT *
    FROM stores
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY sqft DESC) = 1
),
enriched AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
        LAST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft
    FROM ranked
),
enriched_stores AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_share,
        sqft_avg_6_prior,
        last_known_sqft,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft_avg_6_prior NULLS LAST) AS sqft_avg_6_prior_percent_rank,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft_share, id) AS sqft_share_bucket,
        NTILE(10) OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS sqft_bucket,
        SUM(sqft_avg_6_prior) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_avg_6_prior
    FROM enriched
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY sqft_avg_6_prior_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        AVG(sqft) OVER w_grp AS region_avg_sqft
    FROM enriched_stores
    WINDOW w_grp AS (PARTITION BY region)
)
SELECT
    CASE WHEN sqft_share_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS sqft_share_bucket_tier,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT(*) FILTER (WHERE region = 'East') AS east_rows
FROM metrics
GROUP BY CASE WHEN sqft_share_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END
