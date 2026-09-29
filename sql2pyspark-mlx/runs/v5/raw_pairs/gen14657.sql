SELECT
    budget,
    dept_name,
    location,
    UPPER(dept_name) AS dept_name_upper
FROM departments
WHERE dept_name = 'Marketing' AND budget > 3827000;
