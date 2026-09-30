SELECT d.id, d.budget, d.dept_name
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.job_title IN ('Engineer', 'Intern', 'Designer'));
