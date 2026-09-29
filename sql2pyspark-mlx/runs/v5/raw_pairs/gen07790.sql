SELECT
    e.id,
    e.hire_date,
    e.job_title,
    e.full_name
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE budget > 3142000);
