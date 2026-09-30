SELECT
    e.id,
    e.job_title AS employee_job_title,
    m.email AS manager_email,
    m.job_title AS manager_job_title,
    m.hire_date AS manager_hire_date,
    LAST_DAY(e.hire_date) AS hire_date_month_end
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE YEAR(m.hire_date) >= 2023 AND e.email LIKE '%@startup.io';
