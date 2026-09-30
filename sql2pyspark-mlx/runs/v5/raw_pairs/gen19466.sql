SELECT city, AVG(sqft) AS avg_sqft, COUNT(sqft) AS sqft_count
FROM stores
GROUP BY city
ORDER BY avg_sqft, sqft_count DESC;
