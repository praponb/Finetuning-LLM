SELECT
    id,
    budget,
    location,
    dept_name,
    CONCAT(location, ', ', dept_name) AS location_dept_name
FROM departments
