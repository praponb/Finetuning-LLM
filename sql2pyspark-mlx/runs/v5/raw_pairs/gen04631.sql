SELECT e.id, e.level, e.dept_id, e.manager_id FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name IN ('Engineering', 'Sales'));
