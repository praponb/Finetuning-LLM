SELECT d.id, d.dept_name, d.budget
FROM departments d
WHERE d.budget <= 2531000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);
