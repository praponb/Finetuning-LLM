WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        CEIL(salary) AS salary_whole
    FROM employees
    WHERE hire_date > TO_DATE('2023-05-31')
      AND job_title IS NOT NULL
)
-- QUALIFY-style window filter
SELECT
    id,
    manager_id,
    hire_date,
    is_remote
FROM cleaned
QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary_whole) = 1
