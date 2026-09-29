SELECT
    s.*,
    latest.employee_hire_date,
    latest.employee_job_title
FROM departments s
JOIN LATERAL (
        SELECT c.hire_date AS employee_hire_date, c.job_title AS employee_job_title
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.hire_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
