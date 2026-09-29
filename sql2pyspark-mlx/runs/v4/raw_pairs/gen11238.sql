SELECT
    id,
    location,
    dept_name,
    budget
FROM departments
WHERE dept_name NOT IN ('Legal', 'Engineering')
