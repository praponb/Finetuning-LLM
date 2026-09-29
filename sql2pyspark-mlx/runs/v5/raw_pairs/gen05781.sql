WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.level != 'L3'
        )
)
SELECT
    id,
    dept_name,
    budget
FROM kept
