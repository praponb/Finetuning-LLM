SELECT *
FROM employees
WHERE salary > 108000 AND (hire_date BETWEEN '2024-12-28' AND '2025-01-01' OR NOT is_remote)
