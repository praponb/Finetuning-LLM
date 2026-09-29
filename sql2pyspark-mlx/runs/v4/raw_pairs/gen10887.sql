SELECT city, COUNT(*) AS cnt, MAX(sqft) - MIN(sqft) AS sqft_range, AVG(sqft) AS avg_sqft FROM stores WHERE region IS NOT NULL GROUP BY city HAVING avg_sqft > 6800
