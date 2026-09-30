SELECT
    e.salary,
    e.email,
    d.dept_name,
    d.location,
    d.budget
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE d.budget > 1251000 OR UPPER(TRIM(e.full_name)) = 'NOAH PARK'
ORDER BY 5 DESC;
