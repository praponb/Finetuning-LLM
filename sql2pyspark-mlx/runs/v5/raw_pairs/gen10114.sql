SELECT
    e.level AS employee_level,
    m.full_name AS manager_full_name,
    m.email AS manager_email,
    m.hire_date AS manager_hire_date
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.hire_date BETWEEN '2023-04-22' AND '2024-12-01' OR e.manager_id IS NOT NULL
