SELECT d.location, MIN(e.salary) AS min_salary FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.hire_date >= '2024-05-01' GROUP BY d.location
