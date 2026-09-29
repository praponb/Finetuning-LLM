SELECT departments.budget, employees.full_name, employees.hire_date FROM departments LEFT JOIN employees ON departments.id = employees.dept_id WHERE employees.salary > 76000 ORDER BY full_name
