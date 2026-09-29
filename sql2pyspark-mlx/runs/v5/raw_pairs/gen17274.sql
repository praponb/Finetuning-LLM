SELECT d.id, d.dept_name FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE LENGTH(full_name) >= 11)
