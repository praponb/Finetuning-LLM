SELECT order_id, carrier FROM shipments WHERE status = 'LOST' UNION SELECT order_id, carrier FROM shipments WHERE cost <= 59 ORDER BY carrier
