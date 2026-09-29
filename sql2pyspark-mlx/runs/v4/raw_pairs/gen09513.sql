SELECT
    id,
    dept_name,
    budget,
    location
FROM departments
WHERE location IS NOT NULL
