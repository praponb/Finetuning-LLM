SELECT status FROM orders WHERE store_id IN (1, 4, 38) INTERSECT SELECT status FROM subscriptions ORDER BY status DESC;
