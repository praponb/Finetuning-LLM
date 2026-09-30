SELECT e.id, e.is_remote, e.job_title FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget BETWEEN 1568000 AND 3911000);
