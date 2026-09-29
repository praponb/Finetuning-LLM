SELECT CASE WHEN quantity >= 183 THEN 'high' WHEN quantity >= 100 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE YEAR(last_restock) = 2024
GROUP BY quantity_bucket
ORDER BY quantity_bucket DESC
