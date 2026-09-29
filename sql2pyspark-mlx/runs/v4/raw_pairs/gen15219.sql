SELECT employees.id, employees.dept_id FROM employees WHERE employees.dept_id IN (SELECT id FROM departments WHERE budget <= 1798000);
