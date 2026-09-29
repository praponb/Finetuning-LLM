SELECT status, 'payment' source FROM payments UNION ALL SELECT status, 'subscription' source FROM subscriptions ORDER BY source DESC;
