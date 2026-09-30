SELECT id, status, COUNT(*) OVER (PARTITION BY status) AS status_count
FROM subscriptions
WHERE customer_id IS NOT NULL
