SELECT paid_at, order_id FROM payments WHERE amount <= 1540 AND (method = 'paypal' OR paid_at < '2023-04-01')
