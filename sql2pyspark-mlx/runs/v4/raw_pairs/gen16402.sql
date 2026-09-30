SELECT departments.id, departments.budget FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id AND employees.full_name LIKE '%Carol%') ORDER BY budget;
