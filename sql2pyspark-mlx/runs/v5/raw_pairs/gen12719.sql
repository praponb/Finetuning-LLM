WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        region,
        sqft,
        CONCAT(store_name, ' - ', region) AS store_name_region
    FROM stores
    WHERE sqft != 6847
)
-- Multi-level aggregation
SELECT
    region,
    CASE WHEN sqft >= 15256 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY COUNT(CASE WHEN region = 'East' THEN 1 END) DESC) AS east_count_percent_rank
FROM base
GROUP BY region, CASE WHEN sqft >= 15256 THEN 'HIGH' ELSE 'LOW' END
HAVING east_count >= 1
