SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.level AS employee_level,
    m.salary AS manager_salary,
    m.level AS manager_level
FROM employees AS e
LEFT OUTER JOIN employees AS m ON e.manager_id = m.id
ORDER BY employee_level DESC
