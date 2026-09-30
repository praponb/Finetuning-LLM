SELECT region, COUNT(sqft) AS sqft_count, COUNT(DISTINCT sqft) AS unique_citys FROM stores WHERE sqft = 10699 AND store_name IS NOT NULL GROUP BY region
