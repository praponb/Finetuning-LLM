SELECT orders.id, orders.shipping_fee, orders.status
FROM orders
WHERE orders.amount IS NOT NULL AND NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id AND shipments.status NOT IN ('IN_TRANSIT', 'RETURNED'))
