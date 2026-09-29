SELECT 'employee' AS source, hire_date
FROM employees
UNION
SELECT 'subscription' AS source, end_date AS hire_date
FROM subscriptions
WHERE status = 'CANCELLED'
ORDER BY source;
