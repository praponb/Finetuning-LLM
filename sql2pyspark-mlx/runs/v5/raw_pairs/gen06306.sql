SELECT e.id, e.hire_date, d.location
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;
