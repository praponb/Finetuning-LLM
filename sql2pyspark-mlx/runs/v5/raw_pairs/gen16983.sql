SELECT category, ROUND(AVG(cost), 2) avg_cost, SUM(cost) total_cost
FROM products
GROUP BY category
HAVING SUM(cost) < 2000;
