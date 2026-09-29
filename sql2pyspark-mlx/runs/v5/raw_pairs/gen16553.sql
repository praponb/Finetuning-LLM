SELECT status, 'payment' AS source FROM payments WHERE order_id IN (35, 38) UNION ALL SELECT status, 'subscription' AS source FROM subscriptions
