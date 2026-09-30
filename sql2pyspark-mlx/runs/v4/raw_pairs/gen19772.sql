SELECT o.status, MAX(p.amount) AS max_amount, COUNT(DISTINCT p.amount) AS unique_orders
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY o.status;
