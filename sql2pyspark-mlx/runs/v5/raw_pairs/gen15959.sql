SELECT
    s.id,
    s.dept_name,
    x.employee_job_title
FROM departments s
LEFT JOIN LATERAL (
        SELECT c.job_title AS employee_job_title
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.salary DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
