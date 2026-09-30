SELECT d.location, d.budget, d.dept_name, e.email, e.manager_id FROM departments d LEFT JOIN employees e ON d.id = e.dept_id;
