SELECT d.budget, d.dept_name, d.location, e.manager_id, e.hire_date FROM departments d LEFT JOIN employees e ON d.id = e.dept_id
