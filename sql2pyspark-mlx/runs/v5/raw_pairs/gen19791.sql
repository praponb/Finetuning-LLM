SELECT id, order_id, method FROM payments WHERE method NOT IN ('cash', 'paypal') UNION ALL SELECT id, order_id, method FROM payments WHERE amount BETWEEN 420 AND 690;
