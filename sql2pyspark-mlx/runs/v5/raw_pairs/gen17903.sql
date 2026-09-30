SELECT status FROM shipments WHERE shipped_date < '2023-06-01' INTERSECT SELECT status FROM subscriptions WHERE customer_id IN (14, 23, 25) ORDER BY status DESC
