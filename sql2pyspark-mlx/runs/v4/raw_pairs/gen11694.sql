WITH base AS (
    SELECT *, CEIL(budget) AS budget_whole
    FROM departments
    WHERE budget <= 3511000
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM employees ch
        WHERE ch.dept_id = s.id
          AND UPPER(ch.email) LIKE '%IAM%'
    )
