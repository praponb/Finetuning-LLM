SELECT order_id, delivered_date, shipped_date FROM shipments WHERE status = 'LOST' ORDER BY shipped_date DESC
