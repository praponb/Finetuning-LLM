SELECT d.id, d.location
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.manager_id IN (10, 31, 33));
