SELECT
    dept_name,
    budget,
    location,
    LOWER(dept_name) AS dept_name_lower
FROM departments
WHERE location IN ('New York', 'Berlin') AND location IS NULL
