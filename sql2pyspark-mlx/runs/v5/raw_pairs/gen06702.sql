SELECT category, SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END) AS sports_count
FROM products
WHERE product_name NOT LIKE 'Yog%' AND launch_date > '2024-03-01'
GROUP BY category
ORDER BY sports_count DESC;
