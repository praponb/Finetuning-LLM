SELECT
    e.id,
    e.full_name AS employee_full_name,
    e.salary AS employee_salary,
    m.manager_id AS manager_manager_id
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE LENGTH(e.full_name) > 9;
