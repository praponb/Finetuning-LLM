WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        a.*,
        d.level AS employee_level,
        d.job_title AS employee_job_title,
        d.salary AS employee_salary
    FROM employees a
    JOIN employees d
        ON a.manager_id = d.id
)
SELECT
    id,
    email,
    job_title,
    is_remote,
    employee_level,
    employee_job_title
FROM with_dims
ORDER BY employee_level ASC NULLS LAST, job_title ASC
