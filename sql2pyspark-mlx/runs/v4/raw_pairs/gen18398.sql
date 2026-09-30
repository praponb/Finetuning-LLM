SELECT region, AVG(sqft) avg_sqft
FROM stores
WHERE sqft >= 5902
GROUP BY region
ORDER BY region
LIMIT 10
