SELECT departments.id, departments.budget, departments.dept_name
FROM departments
WHERE departments.budget <= 3931000 AND NOT EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id)
ORDER BY departments.id DESC;
