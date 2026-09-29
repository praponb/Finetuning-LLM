SELECT email
FROM customers
UNION ALL
SELECT email
FROM employees
WHERE manager_id IN (2, 3)
ORDER BY email DESC;
