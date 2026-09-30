SELECT d.id, d.dept_name, d.budget
FROM departments d
WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND YEAR(e.hire_date) >= 2025)
ORDER BY dept_name ASC
