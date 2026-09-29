SELECT launch_date
FROM products
EXCEPT
SELECT end_date AS launch_date
FROM subscriptions
WHERE YEAR(end_date) = 2023
ORDER BY launch_date DESC
