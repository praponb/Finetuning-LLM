WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    dept_name,
    location,
    level
FROM latest_match
ORDER BY dept_name NULLS LAST;
