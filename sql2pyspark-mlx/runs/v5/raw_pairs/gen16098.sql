SELECT e.id, e.email FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name = 'Marketing') ORDER BY e.id DESC
