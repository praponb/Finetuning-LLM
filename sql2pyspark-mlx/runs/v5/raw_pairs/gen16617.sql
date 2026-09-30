SELECT region, avg_sqft, stddev_sqft FROM (SELECT region, AVG(sqft) avg_sqft, ROUND(STDDEV(sqft), 2) stddev_sqft FROM stores GROUP BY region) s WHERE stddev_sqft >= 30 ORDER BY stddev_sqft DESC
