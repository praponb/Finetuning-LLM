SELECT customers.is_active, MAX(shipments.shipped_date) AS last_shipped_date, COUNT(DISTINCT shipments.cost) AS unique_orders
FROM shipments
LEFT OUTER JOIN orders ON shipments.order_id = orders.id
LEFT OUTER JOIN customers ON orders.customer_id = customers.id
WHERE orders.shipping_fee > 17
GROUP BY customers.is_active
ORDER BY unique_orders DESC, is_active
LIMIT 20
