SELECT departments.id, departments.dept_name
FROM departments
WHERE departments.dept_name = 'Sales' AND EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id);
