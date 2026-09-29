SELECT
    e.id,
    e.salary employee_salary,
    m.manager_id manager_manager_id,
    m.email manager_email
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.full_name LIKE '%Srisuk%' AND YEAR(m.hire_date) >= 2025
