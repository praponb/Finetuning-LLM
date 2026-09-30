SELECT status, 'payment' AS source FROM payments WHERE TO_DATE(paid_at) >= '2025-07-15' UNION ALL SELECT status, 'subscription' AS source FROM subscriptions
