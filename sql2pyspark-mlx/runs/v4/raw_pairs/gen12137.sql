SELECT order_id, status FROM payments WHERE status != 'FAILED' UNION SELECT order_id, status FROM payments WHERE order_id IS NULL;
