WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT dept_name
    FROM departments
    INTERSECT ALL
    SELECT dept_name
    FROM departments
    WHERE budget < 2164000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN remaining_keys k ON s.dept_name = k.dept_name
)
SELECT dept_name, budget
FROM kept_rows
ORDER BY budget DESC NULLS FIRST
