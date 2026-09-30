SELECT start_date, 'subscription' AS origin
FROM subscriptions
UNION ALL
SELECT hire_date, 'employee' AS origin
FROM employees
