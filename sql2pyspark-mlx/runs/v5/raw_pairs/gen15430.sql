SELECT email
FROM employees
WHERE email LIKE '%@company.co'
UNION
SELECT email
FROM employees
WHERE manager_id IN (2, 19, 23, 38)
