SELECT id, city, sqft, MAX(sqft) OVER (PARTITION BY city) AS city_max_sqft FROM stores WHERE city = 'Bangkok' ORDER BY city_max_sqft;
