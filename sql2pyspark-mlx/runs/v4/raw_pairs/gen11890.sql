SELECT e.manager_id employee_manager_id, m.full_name manager_full_name, m.level manager_level FROM employees e LEFT JOIN employees m ON e.manager_id = m.id WHERE e.level IN ('L2', 'L1', 'L4');
