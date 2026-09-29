SELECT emp.email AS employee_email, mgr.manager_id AS manager_manager_id, mgr.is_remote AS manager_is_remote FROM employees emp LEFT JOIN employees mgr ON emp.manager_id = mgr.id;
