SELECT
    subscriptions.customer_id,
    subscriptions.monthly_fee,
    subscriptions.start_date,
    customers.age,
    customers.segment,
    customers.city
FROM subscriptions
JOIN customers ON customers.id = subscriptions.customer_id
WHERE MONTH(customers.signup_date) IN (3, 4, 11)
