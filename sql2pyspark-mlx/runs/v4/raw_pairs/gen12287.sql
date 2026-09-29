SELECT s.id, s.customer_id, s.plan
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE segment != 'Consumer')
ORDER BY customer_id DESC;
