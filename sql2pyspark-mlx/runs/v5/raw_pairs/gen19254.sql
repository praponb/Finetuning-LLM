SELECT
    e.id,
    e.dept_id AS employee_dept_id,
    e.level AS employee_level,
    e.salary AS employee_salary,
    m.email AS manager_email,
    m.job_title AS manager_job_title
FROM employees AS e
INNER JOIN employees AS m ON e.manager_id = m.id
WHERE e.salary BETWEEN 95000 AND 114000;
