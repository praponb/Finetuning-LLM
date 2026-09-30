WITH base AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        is_remote,
        level,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-07-11'
)
SELECT manager_id
FROM base
EXCEPT ALL
SELECT manager_id
FROM employees
WHERE level = 'L5'
