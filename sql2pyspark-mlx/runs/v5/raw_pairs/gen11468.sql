SELECT e.id, e.salary, e.manager_id FROM employees e WHERE e.is_remote AND e.dept_id IN (SELECT id FROM departments WHERE location IS NOT NULL)
