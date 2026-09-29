SELECT orders.id, orders.channel
FROM orders
WHERE orders.customer_id IS NULL AND EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id)
