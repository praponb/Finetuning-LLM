WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN region IN ('Central', 'North') THEN 'Tier 1' ELSE 'Other' END AS region_group,
        CONCAT(region, ' ', store_name) AS region_store_name
    FROM stores
    WHERE opened_date >= DATE '2023-07-22'
      AND opened_date BETWEEN '2024-02-18' AND '2024-05-13'
)
-- Window metrics over several different partitions
SELECT
    id,
    city,
    opened_date,
    region_group,
    region_store_name,
    NTILE(4) OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS sqft_bucket,
    LEAD(sqft, 1) OVER w_part AS next_sqft,
    COUNT(sqft) OVER w_recent AS city_count_sqft
FROM filtered
WINDOW w_part AS (PARTITION BY region ORDER BY sqft, id), w_recent AS (PARTITION BY city);
