SELECT region, city, AVG(sqft) AS avg_sqft
FROM stores
GROUP BY region, city;
