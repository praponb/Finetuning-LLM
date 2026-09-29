SELECT
    e.id,
    e.full_name AS employee_full_name,
    e.hire_date AS employee_hire_date,
    e.manager_id AS employee_manager_id,
    m.full_name AS manager_full_name
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.level <> 'L1'
