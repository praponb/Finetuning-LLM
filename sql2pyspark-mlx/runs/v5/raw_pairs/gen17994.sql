SELECT e.id, e.salary AS employee_salary, m.salary AS manager_salary, m.manager_id AS manager_manager_id FROM employees e INNER JOIN employees m ON e.manager_id = m.id WHERE m.dept_id IN (20, 24, 32)
