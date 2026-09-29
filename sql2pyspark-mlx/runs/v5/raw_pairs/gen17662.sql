SELECT orders.id, orders.customer_id, orders.status, orders.store_id FROM orders WHERE NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id AND shipments.carrier = 'Flash');
