SELECT e.full_name AS emp_name, m.full_name AS manager_name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id;
