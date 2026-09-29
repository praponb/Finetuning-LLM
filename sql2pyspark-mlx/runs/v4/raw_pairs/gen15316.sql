SELECT
    id,
    manager_id,
    salary,
    LOWER(full_name) AS full_name_lower
FROM employees
WHERE manager_id = 12
