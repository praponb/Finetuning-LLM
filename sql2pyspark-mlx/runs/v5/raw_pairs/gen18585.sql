SELECT
    id,
    dept_name,
    budget,
    location,
    FIRST_VALUE(location) OVER (PARTITION BY dept_name ORDER BY budget, id) AS first_location
FROM departments
