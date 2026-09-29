SELECT e.id, e.dept_id, e.is_remote, e.manager_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location IN ('New York', 'London'))
