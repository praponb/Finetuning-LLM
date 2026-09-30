SELECT start_date
FROM subscriptions
EXCEPT
SELECT signup_date AS start_date
FROM customers
WHERE is_active = FALSE
