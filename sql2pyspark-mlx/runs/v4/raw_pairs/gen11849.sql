WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM employees
    INTERSECT ALL
    SELECT level
    FROM employees
    WHERE job_title IN ('Manager', 'Analyst', 'Director')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    JOIN remaining_keys k ON s.level = k.level
)
SELECT
    full_name,
    email,
    job_title,
    hire_date,
    is_remote,
    level
FROM verified
ORDER BY job_title DESC NULLS LAST, hire_date ASC;
