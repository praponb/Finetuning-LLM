SELECT d.id, d.location
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND YEAR(e.hire_date) = 2025);
