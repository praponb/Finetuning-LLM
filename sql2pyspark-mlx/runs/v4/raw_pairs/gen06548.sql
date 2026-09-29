SELECT status
FROM orders
UNION ALL
SELECT status
FROM subscriptions
WHERE YEAR(end_date) = 2024
ORDER BY status
