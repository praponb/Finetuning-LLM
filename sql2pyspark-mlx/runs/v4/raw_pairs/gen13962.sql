SELECT departments.location, COUNT(*) AS num_employees FROM employees RIGHT JOIN departments ON departments.id = employees.dept_id GROUP BY departments.location ORDER BY departments.location
