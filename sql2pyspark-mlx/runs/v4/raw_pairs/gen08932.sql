SELECT category, SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END) AS umbrella_count
FROM products
GROUP BY category
ORDER BY 1, umbrella_count DESC
