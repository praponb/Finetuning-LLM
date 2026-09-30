SELECT orders.id, orders.discount, orders.order_date
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id AND shipments.status = 'LOST')
ORDER BY order_date, orders.discount;
