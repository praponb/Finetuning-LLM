SELECT d.id, d.dept_name, d.budget
FROM departments d
WHERE d.dept_name IN ('Legal', 'Engineering', 'Finance') AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.manager_id IS NULL)
ORDER BY id DESC
