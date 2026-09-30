SELECT
    e.id,
    e.full_name AS employee_full_name,
    e.is_remote AS employee_is_remote,
    e.hire_date AS employee_hire_date,
    m.hire_date AS manager_hire_date
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id;
