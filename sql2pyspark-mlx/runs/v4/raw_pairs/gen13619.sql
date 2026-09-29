SELECT subscriptions.plan, customers.segment
FROM subscriptions
INNER JOIN customers ON customers.id = subscriptions.customer_id
ORDER BY plan;
