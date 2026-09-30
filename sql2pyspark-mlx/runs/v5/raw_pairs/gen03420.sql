SELECT d.id, d.location, COUNT(e.id) AS employee_count FROM departments d LEFT OUTER JOIN employees e ON d.id = e.dept_id WHERE d.dept_name = 'Engineering' GROUP BY d.id, d.location;
