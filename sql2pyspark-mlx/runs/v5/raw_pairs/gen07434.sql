SELECT id, method, order_id FROM payments WHERE amount > 1000 UNION SELECT id, method, order_id FROM payments WHERE method IS NOT NULL;
