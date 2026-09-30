SELECT
    id,
    location,
    dept_name,
    COUNT(*) OVER (PARTITION BY location, dept_name) AS location_count
FROM departments
WHERE location = 'London';
