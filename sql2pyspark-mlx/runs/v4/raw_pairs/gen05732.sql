SELECT
    e.is_remote AS employee_is_remote,
    e.salary AS employee_salary,
    m.manager_id AS manager_manager_id,
    m.dept_id AS manager_dept_id,
    m.full_name AS manager_full_name
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE UPPER(TRIM(e.email)) = 'CAROL@COMPANY.CO'
ORDER BY manager_manager_id DESC;
