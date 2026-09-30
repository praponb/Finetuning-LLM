SELECT
    e.id,
    e.email AS employee_email,
    e.full_name AS employee_full_name,
    e.manager_id AS employee_manager_id,
    m.manager_id AS manager_manager_id,
    m.email AS manager_email
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.level IN ('L5', 'L2')
