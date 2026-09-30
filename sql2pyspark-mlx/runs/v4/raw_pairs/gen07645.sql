SELECT
    e.id,
    e.salary AS employee_salary,
    e.email AS employee_email,
    m.level AS manager_level,
    m.hire_date AS manager_hire_date
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.email LIKE '%@company.co'
