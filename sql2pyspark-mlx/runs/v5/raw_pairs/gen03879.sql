SELECT e.id, e.email, e.job_title
FROM employees e
WHERE e.dept_id IN (1, 8, 12) AND e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Engineering')
ORDER BY email ASC;
