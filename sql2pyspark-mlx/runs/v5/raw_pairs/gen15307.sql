SELECT
    e.id,
    e.dept_id AS employee_dept_id,
    e.is_remote AS employee_is_remote,
    m.hire_date AS manager_hire_date,
    m.job_title AS manager_job_title
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.salary < 87000
ORDER BY manager_job_title;
