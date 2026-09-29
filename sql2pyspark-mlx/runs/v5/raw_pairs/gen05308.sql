SELECT status FROM orders INTERSECT SELECT status FROM shipments ORDER BY status DESC;
