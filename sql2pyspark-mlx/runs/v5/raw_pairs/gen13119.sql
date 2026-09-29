WITH enriched AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.dept_id,
        a.job_title,
        a.salary,
        a.is_remote,
        a.level,
        b.location,
        b.dept_name
    FROM employees a
    LEFT JOIN departments b
        ON a.dept_id = b.id
)
SELECT
    id,
    email,
    dept_id,
    salary,
    is_remote,
    level,
    dept_name
FROM enriched
ORDER BY salary
