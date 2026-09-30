SELECT shipped_date AS event_date
FROM shipments
UNION ALL
SELECT hire_date AS event_date
FROM employees
WHERE is_remote;
