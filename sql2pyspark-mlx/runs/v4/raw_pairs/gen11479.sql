SELECT region, city, AVG(sqft) AS avg_sqft
FROM stores
WHERE MONTH(opened_date) IN (5, 6, 11) AND (store_name LIKE '%Old%' OR region <> 'North')
GROUP BY region, city
HAVING AVG(sqft) > 10300
ORDER BY city, avg_sqft DESC
