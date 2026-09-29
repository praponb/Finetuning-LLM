SELECT *, COALESCE(full_name, 'N/A') AS full_name_filled, LENGTH(level) AS level_length
FROM employees
WHERE hire_date < '2024-10-05';
