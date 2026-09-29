SELECT status FROM orders WHERE store_id = 27 INTERSECT SELECT status FROM shipments ORDER BY status DESC
