SELECT
    e.id,
    e.salary,
    e.is_remote,
    e.job_title,
    d.dept_name,
    d.budget
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE d.budget BETWEEN 1201000 AND 1403000 AND d.location IN ('Bangkok', 'New York', 'Singapore');
