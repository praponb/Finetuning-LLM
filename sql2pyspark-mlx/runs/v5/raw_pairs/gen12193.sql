SELECT
    e.id,
    e.salary,
    e.job_title,
    e.dept_id
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE budget > 2274000)
ORDER BY job_title DESC
