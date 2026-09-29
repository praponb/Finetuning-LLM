SELECT
    s.id,
    s.status,
    s.end_date,
    s.customer_id
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE signup_date >= '2023-08-01');
