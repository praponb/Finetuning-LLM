SELECT
    subscriptions.id,
    subscriptions.plan,
    subscriptions.customer_id,
    subscriptions.end_date
FROM subscriptions
WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE name LIKE '%Rossi%');
