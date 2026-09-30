SELECT method, status FROM payments WHERE method IN ('paypal', 'bank_transfer', 'card') UNION SELECT method, status FROM payments WHERE paid_at < '2024-11-01'
