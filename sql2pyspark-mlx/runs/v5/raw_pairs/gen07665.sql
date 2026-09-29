SELECT e.id, e.manager_id AS employee_manager_id, e.full_name AS employee_full_name, m.manager_id AS manager_manager_id FROM employees AS e LEFT JOIN employees AS m ON e.manager_id = m.id;
