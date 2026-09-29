SELECT opened_date
FROM accounts
INTERSECT
SELECT hire_date AS opened_date
FROM employees
ORDER BY opened_date
