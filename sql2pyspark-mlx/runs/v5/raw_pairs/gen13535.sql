SELECT signup_date AS event_date, 'customer' AS record_type
FROM customers
UNION ALL
SELECT end_date AS event_date, 'subscription' AS record_type
FROM subscriptions
WHERE plan IN ('basic', 'standard')
ORDER BY event_date
