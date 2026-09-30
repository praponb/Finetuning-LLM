SELECT region, COUNT(sqft) AS sqft_count, MIN(opened_date) AS first_opened_date
FROM stores
WHERE region IN ('South', 'North', 'Central')
GROUP BY region
ORDER BY 1 DESC
LIMIT 10;
