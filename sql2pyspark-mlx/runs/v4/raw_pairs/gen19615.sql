SELECT id, status, delivered_date, cost FROM shipments WHERE order_id IS NULL AND status = 'RETURNED' AND MONTH(delivered_date) IN (5, 6, 12) ORDER BY id DESC, cost DESC LIMIT 5;
