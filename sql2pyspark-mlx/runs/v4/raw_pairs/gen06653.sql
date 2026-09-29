SELECT hire_date AS event_date
FROM employees
WHERE hire_date < '2025-10-01'
INTERSECT
SELECT last_restock AS event_date
FROM inventory;
