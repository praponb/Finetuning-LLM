SELECT d.location, d.budget, e.is_remote, e.full_name FROM departments d LEFT OUTER JOIN employees e ON d.id = e.dept_id WHERE MONTH(e.hire_date) IN (3, 4, 5)
