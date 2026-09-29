SELECT email
FROM customers
WHERE NOT is_active
INTERSECT
SELECT email
FROM employees
WHERE MONTH(hire_date) IN (5, 11, 12)
