SELECT * FROM (SELECT region, AVG(sqft) AS avg_sqft FROM stores GROUP BY region) AS t WHERE avg_sqft < 15300 ORDER BY avg_sqft DESC, region LIMIT 1;
