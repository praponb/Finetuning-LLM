SELECT hire_date AS event_date
FROM employees
WHERE salary >= 58000
INTERSECT
SELECT opened_date AS event_date
FROM stores
ORDER BY event_date;
