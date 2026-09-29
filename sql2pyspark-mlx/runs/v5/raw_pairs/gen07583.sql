SELECT d.id, d.budget
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.manager_id IS NOT NULL)
ORDER BY id;
