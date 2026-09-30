WITH matched_keys AS (
    SELECT level, job_title
    FROM employees
    EXCEPT
    SELECT level, job_title
    FROM employees
    WHERE email NOT LIKE '%@startup.io'
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    JOIN matched_keys k ON s.level = k.level AND s.job_title = k.job_title
)
SELECT
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    is_remote,
    level
FROM kept_rows;
