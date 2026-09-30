SELECT hire_date
FROM employees
WHERE is_remote
EXCEPT
SELECT start_date AS hire_date
FROM subscriptions
WHERE YEAR(end_date) >= 2024
ORDER BY hire_date
