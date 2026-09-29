SELECT employees.hire_date, employees.level, departments.dept_name, departments.budget, departments.location FROM employees INNER JOIN departments ON employees.dept_id = departments.id
