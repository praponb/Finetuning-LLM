SELECT
    e.id,
    e.dept_id,
    e.job_title,
    d.location,
    d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE e.is_remote OR e.salary BETWEEN 90000 AND 95000
ORDER BY job_title DESC;
