SELECT
    subscriptions.id,
    subscriptions.plan,
    customers.age,
    customers.signup_date,
    customers.country
FROM subscriptions
LEFT JOIN customers ON subscriptions.customer_id = customers.id
WHERE customers.signup_date < '2024-10-21'
ORDER BY subscriptions.id DESC;
