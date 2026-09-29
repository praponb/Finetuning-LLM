SELECT
    e.id,
    e.manager_id employee_manager_id,
    e.hire_date employee_hire_date,
    e.level employee_level,
    m.is_remote manager_is_remote,
    m.level manager_level
FROM employees e
JOIN employees m ON e.manager_id = m.id;
