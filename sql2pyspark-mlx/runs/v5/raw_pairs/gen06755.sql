SELECT departments.id, departments.location, departments.budget, departments.dept_name FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id);
