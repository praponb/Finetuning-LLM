SELECT id, order_id, carrier FROM shipments WHERE delivered_date <= '2025-07-28' UNION SELECT id, order_id, carrier FROM shipments WHERE order_id IN (2, 11, 29, 38);
