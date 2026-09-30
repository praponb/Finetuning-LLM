SELECT id, region, sqft, ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 3) AS sqft_share FROM stores WHERE store_name IS NOT NULL;
