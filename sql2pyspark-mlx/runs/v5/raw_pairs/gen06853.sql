SELECT
    e.salary AS employee_salary,
    e.dept_id AS employee_dept_id,
    e.email AS employee_email,
    m.email AS manager_email
FROM employees AS e
INNER JOIN employees AS m ON e.manager_id = m.id
