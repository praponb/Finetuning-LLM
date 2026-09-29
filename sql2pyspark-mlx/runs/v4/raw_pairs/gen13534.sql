SELECT region, SUM(sqft) AS total_sqft, DENSE_RANK() OVER (ORDER BY SUM(sqft) DESC) AS rnk
FROM stores
WHERE sqft <> 13946
GROUP BY region
ORDER BY total_sqft DESC, region
LIMIT 5
