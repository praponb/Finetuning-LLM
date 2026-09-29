SELECT status, 'order' AS source FROM orders UNION SELECT status, 'payment' AS source FROM payments WHERE method = 'bank_transfer';
