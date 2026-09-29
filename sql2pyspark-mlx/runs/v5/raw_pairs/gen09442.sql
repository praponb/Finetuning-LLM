SELECT delivered_date
FROM shipments
UNION
SELECT hire_date
FROM employees
ORDER BY delivered_date DESC;
