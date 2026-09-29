SELECT e.id, e.job_title
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE location IS NULL);
