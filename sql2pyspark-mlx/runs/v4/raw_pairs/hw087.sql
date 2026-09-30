SELECT id, amount FROM orders WHERE channel = 'web' UNION ALL SELECT id, amount FROM payments WHERE status = 'FAILED';
