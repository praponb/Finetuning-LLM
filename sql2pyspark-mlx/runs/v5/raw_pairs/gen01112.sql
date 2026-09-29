SELECT d.location, d.dept_name, e.full_name, e.email, e.dept_id FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id WHERE d.budget < 2140000 ORDER BY dept_id DESC
