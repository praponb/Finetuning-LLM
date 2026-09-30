SELECT
    subscriptions.id,
    subscriptions.status,
    subscriptions.monthly_fee,
    customers.signup_date
FROM subscriptions
JOIN customers ON subscriptions.customer_id = customers.id;
