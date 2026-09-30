WITH base AS (
    SELECT *, LENGTH(dept_name) AS dept_name_length
    FROM departments
    WHERE budget BETWEEN 2815000 AND 3447000
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM employees ch
        WHERE ch.dept_id = s.id
          AND ch.dept_id IN (5, 31, 32, 36)
    );
