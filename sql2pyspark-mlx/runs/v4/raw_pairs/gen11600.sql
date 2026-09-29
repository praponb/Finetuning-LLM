WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM stores
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    CASE WHEN cumulative_sqft >= 87393 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_sqft_tier,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    ROUND(SUM(sqft_share) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS sqft_share_per_north,
    RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date) ORDER BY ROUND(SUM(sqft_share) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) DESC) AS sqft_share_per_north_rank
FROM metrics
GROUP BY DATE_TRUNC('MONTH', opened_date), CASE WHEN cumulative_sqft >= 87393 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY cumulative_sqft_tier DESC
