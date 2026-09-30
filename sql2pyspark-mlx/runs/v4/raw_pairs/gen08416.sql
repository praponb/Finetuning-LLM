SELECT departments.location, departments.dept_name, departments.budget, employees.level, employees.dept_id FROM departments LEFT OUTER JOIN employees ON employees.dept_id = departments.id;
