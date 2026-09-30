SELECT
    s.id,
    s.start_date,
    s.status,
    s.plan
FROM subscriptions s
WHERE s.customer_id = 18 AND s.customer_id IN (SELECT id FROM customers WHERE segment = 'Corporate');
