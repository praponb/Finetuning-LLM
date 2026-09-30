SELECT d.dept_name, d.budget, d.location, e.is_remote, e.dept_id, e.salary FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE YEAR(e.hire_date) = 2025 AND e.level NOT IN ('L3', 'L1')
