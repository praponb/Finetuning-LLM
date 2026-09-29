SELECT e.id, e.salary, e.full_name, e.is_remote FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name = 'Engineering') ORDER BY salary DESC, e.full_name DESC;
