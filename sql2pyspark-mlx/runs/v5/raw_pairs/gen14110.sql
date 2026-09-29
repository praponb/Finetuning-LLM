SELECT 'order' AS record_type, amount FROM orders WHERE customer_id IN (8, 13, 15, 23) UNION SELECT 'product' AS record_type, cost AS amount FROM products ORDER BY amount;
