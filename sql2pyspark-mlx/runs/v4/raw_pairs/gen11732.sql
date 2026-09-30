SELECT d.location, d.dept_name, d.budget, e.email FROM departments d JOIN employees e ON e.dept_id = d.id WHERE e.level IN ('L5', 'L3')
