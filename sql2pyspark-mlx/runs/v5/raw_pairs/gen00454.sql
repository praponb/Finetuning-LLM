SELECT order_id, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
GROUP BY order_id
ORDER BY order_id DESC, total_cost
LIMIT 10
