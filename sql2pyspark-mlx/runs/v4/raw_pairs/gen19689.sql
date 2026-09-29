SELECT departments.budget, employees.dept_id FROM departments LEFT JOIN employees ON departments.id = employees.dept_id WHERE employees.manager_id = 6;
