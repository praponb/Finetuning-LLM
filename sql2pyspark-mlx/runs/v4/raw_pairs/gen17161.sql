SELECT status, 'payment' AS source FROM payments WHERE HOUR(paid_at) BETWEEN 7 AND 10 UNION ALL SELECT status, 'subscription' AS source FROM subscriptions
