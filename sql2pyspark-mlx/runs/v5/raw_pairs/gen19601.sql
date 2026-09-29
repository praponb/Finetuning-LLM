SELECT hire_date
FROM employees
UNION
SELECT last_restock
FROM inventory
ORDER BY hire_date DESC;
