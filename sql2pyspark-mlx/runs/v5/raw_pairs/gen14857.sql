SELECT delivered_date AS event_date FROM shipments INTERSECT SELECT hire_date AS event_date FROM employees WHERE email IS NOT NULL ORDER BY event_date;
