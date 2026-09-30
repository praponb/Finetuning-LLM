SELECT e.id, e.email, d.location
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE d.dept_name = 'Legal' AND d.dept_name IN ('Engineering', 'Sales', 'HR')
