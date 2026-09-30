SELECT
    location,
    dept_name,
    CONCAT(location, '/', dept_name) AS location_dept_name,
    UPPER(dept_name) AS dept_name_upper
FROM departments
WHERE budget <= 1846000 AND budget IS NOT NULL AND location <> 'Bangkok';
