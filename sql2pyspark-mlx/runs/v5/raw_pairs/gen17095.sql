SELECT status FROM payments WHERE status = 'SUCCESS' INTERSECT SELECT status FROM shipments ORDER BY status DESC;
