SELECT start_date
FROM subscriptions
UNION
SELECT signup_date
FROM customers
WHERE signup_date > '2025-07-04'
ORDER BY start_date;
