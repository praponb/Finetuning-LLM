SELECT d.budget, e.is_remote, e.level, e.email FROM departments d INNER JOIN employees e ON d.id = e.dept_id WHERE d.budget BETWEEN 2941000 AND 3181000
