SELECT
    e.id,
    e.manager_id,
    e.email,
    d.location,
    d.dept_name,
    TRIM(e.job_title) AS clean_job_title
FROM employees e
JOIN departments d ON e.dept_id = d.id
ORDER BY clean_job_title DESC;
