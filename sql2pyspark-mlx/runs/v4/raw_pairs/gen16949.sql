SELECT id, order_id FROM payments WHERE order_id IN (1, 9, 16, 24) INTERSECT SELECT id, order_id FROM payments WHERE status = 'SUCCESS';
