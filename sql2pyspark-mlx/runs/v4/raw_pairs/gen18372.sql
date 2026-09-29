SELECT
    subscriptions.start_date,
    subscriptions.customer_id,
    customers.signup_date,
    customers.segment,
    customers.city,
    CONCAT_WS(',', customers.email, customers.country) AS email_country_key
FROM subscriptions
LEFT JOIN customers ON subscriptions.customer_id = customers.id
WHERE customers.city IN ('Chiang Mai', 'Tokyo', 'Berlin') AND subscriptions.start_date >= '2024-11-14'
