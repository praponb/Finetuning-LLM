SELECT
    id,
    salary,
    level,
    LOWER(full_name) AS full_name_lower,
    ROUND(salary * 0.9, 2) AS salary_scaled
FROM employees
WHERE dept_id IN (26, 29) AND dept_id IN (12, 26, 32, 40)
ORDER BY level, id
LIMIT 20;
