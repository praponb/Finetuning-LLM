SELECT employees.id, employees.full_name FROM employees WHERE employees.dept_id NOT IN (SELECT id FROM departments WHERE dept_name NOT IN ('Finance', 'Sales'))
