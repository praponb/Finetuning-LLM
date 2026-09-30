SELECT orders.status, MONTH(orders.order_date) AS order_month, MAX(shipments.cost) - MIN(shipments.cost) AS cost_range
FROM shipments
LEFT JOIN orders ON shipments.order_id = orders.id
FULL OUTER JOIN customers ON customers.id = orders.customer_id
GROUP BY orders.status, MONTH(orders.order_date);
