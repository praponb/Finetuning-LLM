SELECT employees.hire_date, departments.dept_name FROM employees INNER JOIN departments ON employees.dept_id = departments.id WHERE departments.location = 'Bangkok';
