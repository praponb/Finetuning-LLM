SELECT d.location, COUNT(e.id) employee_count, MAX(e.salary) max_salary FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.location ORDER BY employee_count, location LIMIT 5;
