WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        UPPER(TRIM(email)) AS email_normalized
    FROM employees
    WHERE MONTH(hire_date) IN (3, 8, 11)
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    manager_id,
    job_title,
    level
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC NULLS FIRST, id) = 1;
