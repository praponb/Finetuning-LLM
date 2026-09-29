WITH enriched AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.email,
        e.dept_id,
        e.manager_id,
        e.job_title,
        e.salary,
        e.hire_date,
        e.is_remote,
        b.job_title AS employee_job_title,
        b.full_name AS employee_full_name
    FROM employees e
    INNER JOIN employees b
        ON e.manager_id = b.id
)
SELECT
    id,
    email,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    employee_job_title,
    employee_full_name,
    CASE WHEN employee_job_title RLIKE '^(Director|Engineer)$' THEN 'GROUP_B' WHEN employee_job_title RLIKE '^(Analyst|Manager)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS employee_job_title_zone
FROM enriched
