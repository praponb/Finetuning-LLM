SELECT id, status FROM payments WHERE status IN ('FAILED', 'REFUNDED') UNION ALL SELECT id, status FROM payments WHERE method IN ('paypal', 'cash', 'bank_transfer')
