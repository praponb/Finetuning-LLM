SELECT
    e.id,
    e.level AS employee_level,
    m.is_remote AS manager_is_remote,
    m.full_name AS manager_full_name
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.email LIKE '%@outlook.com';
