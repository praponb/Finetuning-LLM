SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.level AS employee_level,
    e.email AS employee_email,
    m.manager_id AS manager_manager_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.is_remote OR e.level = 'L1'
ORDER BY employee_email DESC;
