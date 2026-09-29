SELECT 'account' AS kind, opened_date
FROM accounts
WHERE account_type IN ('CREDIT', 'LOAN')
UNION
SELECT 'employee' AS kind, hire_date
FROM employees
WHERE salary BETWEEN 94000 AND 130000
