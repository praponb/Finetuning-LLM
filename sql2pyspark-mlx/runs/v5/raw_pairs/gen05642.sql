SELECT COUNT(sqft) AS sqft_count, MAX(sqft) AS max_sqft FROM stores WHERE city = 'Tokyo' AND region IS NOT NULL;
