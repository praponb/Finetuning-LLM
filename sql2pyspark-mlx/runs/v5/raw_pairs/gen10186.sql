SELECT status FROM orders WHERE store_id IS NULL UNION ALL SELECT status FROM subscriptions
