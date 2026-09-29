SELECT d.dept_name, MIN(e.hire_date) AS first_hire_date FROM employees e LEFT JOIN departments d ON d.id = e.dept_id GROUP BY d.dept_name
