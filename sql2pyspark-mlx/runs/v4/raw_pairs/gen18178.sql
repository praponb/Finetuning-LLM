SELECT id, cost, carrier, order_id FROM shipments WHERE order_id IS NULL AND (order_id = 28 OR MONTH(delivered_date) IN (3, 7, 11))
