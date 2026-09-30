SELECT
    e.id,
    e.full_name AS employee_full_name,
    e.email AS employee_email,
    e.manager_id AS employee_manager_id,
    m.is_remote AS manager_is_remote,
    m.manager_id AS manager_manager_id
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
ORDER BY e.full_name, id
LIMIT 5;
