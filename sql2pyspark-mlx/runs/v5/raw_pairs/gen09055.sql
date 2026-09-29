SELECT order_id, status FROM shipments WHERE order_id = 12 INTERSECT SELECT order_id, status FROM shipments WHERE cost BETWEEN 30 AND 51 ORDER BY status;
