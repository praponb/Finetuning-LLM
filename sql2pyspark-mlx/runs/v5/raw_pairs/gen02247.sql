SELECT id, shipped_date FROM shipments WHERE order_id IN (19, 27, 37) UNION SELECT id, shipped_date FROM shipments WHERE carrier = 'Flash' ORDER BY shipped_date;
