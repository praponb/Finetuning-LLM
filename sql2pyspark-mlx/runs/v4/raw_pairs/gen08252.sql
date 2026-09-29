WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM employees
    EXCEPT
    SELECT id
    FROM employees
    WHERE full_name LIKE '%Mia%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    full_name,
    email,
    dept_id,
    job_title,
    level
FROM reconciled
ORDER BY job_title DESC NULLS FIRST, dept_id
