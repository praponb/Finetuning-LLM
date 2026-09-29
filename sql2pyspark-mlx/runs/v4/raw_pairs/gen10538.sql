SELECT status, 'payment' AS source FROM payments WHERE method IN ('cash', 'bank_transfer', 'card') UNION SELECT status, 'subscription' AS source FROM subscriptions ORDER BY source;
