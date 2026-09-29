WITH recent_stores AS (
    SELECT *, LENGTH(region) AS region_length
    FROM stores
    WHERE (LENGTH(store_name) > 6 AND city IN ('New York', 'Chiang Mai', 'London')) OR sqft <= 13633
),
stores_stats AS (
    SELECT CASE WHEN region IN ('Central', 'North') THEN 'tier_1' ELSE 'other' END AS region_group, SUM(CASE WHEN region = 'North' THEN region_length ELSE 0 END) AS north_region_length
    FROM recent_stores
    GROUP BY CASE WHEN region IN ('Central', 'North') THEN 'tier_1' ELSE 'other' END
)
SELECT region_group, north_region_length
FROM stores_stats
ORDER BY region_group;
