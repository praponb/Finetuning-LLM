SELECT status FROM orders WHERE store_id IS NULL INTERSECT SELECT status FROM subscriptions
