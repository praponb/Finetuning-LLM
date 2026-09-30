SELECT category, SUM(cost) total_cost
FROM products
GROUP BY category
HAVING SUM(cost) > 1200
ORDER BY category
LIMIT 20;
