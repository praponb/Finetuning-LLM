SELECT price, 'product' AS origin FROM products UNION SELECT amount AS price, 'transaction' AS origin FROM transactions;
