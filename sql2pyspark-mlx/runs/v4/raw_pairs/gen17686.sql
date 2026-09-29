SELECT level
FROM employees
WHERE full_name IS NOT NULL
UNION
SELECT level
FROM employees
WHERE salary > 105000
