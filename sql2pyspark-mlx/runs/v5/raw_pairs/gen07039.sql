SELECT 'account' AS source, opened_date
FROM accounts
UNION
SELECT 'employee' AS source, hire_date
FROM employees;
