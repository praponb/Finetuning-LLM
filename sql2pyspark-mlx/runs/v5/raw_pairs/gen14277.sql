SELECT e.id, e.salary, e.dept_id, d.budget FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.hire_date < '2023-12-04'
