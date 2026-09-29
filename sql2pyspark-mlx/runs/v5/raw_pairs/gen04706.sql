SELECT e.id, e.is_remote employee_is_remote, m.hire_date manager_hire_date, m.is_remote manager_is_remote FROM employees e INNER JOIN employees m ON e.manager_id = m.id;
