SELECT paid_at, order_id, UPPER(method) AS method_upper
FROM payments
WHERE paid_at >= '2023-08-22' OR method = 'bank_transfer';
