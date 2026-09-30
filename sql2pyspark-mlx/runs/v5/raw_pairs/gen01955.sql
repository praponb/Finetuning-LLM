SELECT emp.id, emp.email AS employee_email, mgr.is_remote AS manager_is_remote FROM employees emp LEFT JOIN employees mgr ON emp.manager_id = mgr.id ORDER BY emp.email
