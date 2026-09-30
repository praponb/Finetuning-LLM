SELECT e.id, e.dept_id AS employee_dept_id, e.full_name AS employee_full_name, m.level AS manager_level FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.full_name;
