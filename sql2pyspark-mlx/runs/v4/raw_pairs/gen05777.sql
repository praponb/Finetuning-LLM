SELECT
    e.salary AS employee_salary,
    e.level AS employee_level,
    m.email AS manager_email,
    m.job_title AS manager_job_title,
    m.hire_date AS manager_hire_date
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
ORDER BY manager_job_title;
