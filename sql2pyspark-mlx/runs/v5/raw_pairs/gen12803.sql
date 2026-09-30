SELECT id, carrier, cost FROM shipments WHERE order_id IN (23, 31, 36, 38) AND (order_id IS NOT NULL OR delivered_date < '2024-01-01')
