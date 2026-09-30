SELECT cost AS value, 'product' AS source FROM products WHERE supplier_id IS NULL UNION SELECT amount AS value, 'transaction' AS source FROM transactions;
