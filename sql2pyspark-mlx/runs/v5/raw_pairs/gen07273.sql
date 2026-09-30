SELECT c.is_active, ROUND(AVG(o.discount), 2) AS avg_discount, MAX(o.order_date) AS last_order_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.is_active
HAVING avg_discount > 47;
