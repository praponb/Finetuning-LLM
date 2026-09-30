SELECT s.id, s.status, s.start_date
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE LENGTH(name) > 13)
ORDER BY status DESC
