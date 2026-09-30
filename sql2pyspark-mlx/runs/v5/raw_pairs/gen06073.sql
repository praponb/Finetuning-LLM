SELECT e.id, e.salary AS employee_salary, m.level AS manager_level FROM employees e LEFT OUTER JOIN employees m ON e.manager_id = m.id WHERE e.dept_id IN (8, 25) ORDER BY m.level;
