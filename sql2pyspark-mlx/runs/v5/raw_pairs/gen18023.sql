SELECT status FROM payments WHERE method = 'paypal' UNION ALL SELECT status FROM subscriptions;
