SELECT d.budget, e.full_name, e.email, e.is_remote FROM departments d LEFT JOIN employees e ON e.dept_id = d.id ORDER BY budget ASC, full_name ASC;
