SELECT e.id, e.dept_id AS employee_dept_id, e.is_remote AS employee_is_remote, m.level AS manager_level FROM employees e JOIN employees m ON e.manager_id = m.id ORDER BY manager_level;
