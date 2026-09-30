SELECT cost, 'product' record_type FROM products UNION ALL SELECT amount, 'order' record_type FROM orders ORDER BY record_type;
