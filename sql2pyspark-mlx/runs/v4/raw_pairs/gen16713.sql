SELECT d.id, d.location, COUNT(e.id) AS employee_count, MAX(e.salary) AS max_salary FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.id, d.location ORDER BY d.location, id DESC
