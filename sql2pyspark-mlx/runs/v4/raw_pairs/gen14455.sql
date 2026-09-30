SELECT
    e.id,
    e.is_remote AS employee_is_remote,
    e.dept_id AS employee_dept_id,
    e.full_name AS employee_full_name,
    m.manager_id AS manager_manager_id,
    m.hire_date AS manager_hire_date,
    m.level AS manager_level
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE LENGTH(m.email) < 8
