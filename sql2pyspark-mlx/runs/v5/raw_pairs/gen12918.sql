SELECT d.location, d.budget, e.manager_id, e.is_remote, e.full_name FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.email LIKE '%@mail.co.th';
