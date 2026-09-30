SELECT
    e.id,
    e.email AS employee_email,
    e.salary AS employee_salary,
    e.job_title AS employee_job_title,
    m.salary AS manager_salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;
