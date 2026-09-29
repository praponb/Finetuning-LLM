SELECT d.id, d.dept_name, d.budget, d.location FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND LOWER(e.email) LIKE '%aro%') ORDER BY d.id DESC
