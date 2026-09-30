SELECT 'inventory' AS kind, last_restock AS event_date
FROM inventory
UNION ALL
SELECT 'employee' AS kind, hire_date AS event_date
FROM employees
ORDER BY event_date DESC
