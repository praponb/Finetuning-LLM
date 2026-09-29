SELECT id, method, order_id FROM payments WHERE amount BETWEEN 760 AND 1500 AND (order_id IN (10, 15, 23) OR order_id IS NULL) ORDER BY method ASC;
