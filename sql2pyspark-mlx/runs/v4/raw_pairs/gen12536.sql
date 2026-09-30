SELECT signup_date AS event_date, 'customer' AS source
FROM customers
WHERE is_active = TRUE
UNION ALL
SELECT start_date AS event_date, 'subscription' AS source
FROM subscriptions
