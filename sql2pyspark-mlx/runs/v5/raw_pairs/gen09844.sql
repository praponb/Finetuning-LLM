SELECT hire_date AS event_date
FROM employees
UNION
SELECT last_restock AS event_date
FROM inventory;
