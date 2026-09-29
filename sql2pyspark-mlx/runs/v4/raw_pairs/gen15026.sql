WITH base AS (
    SELECT *, FLOOR(budget) AS budget_whole
    FROM departments
    WHERE budget >= 3212000
)
SELECT s.*, x.job_title
FROM base s
JOIN LATERAL (
        SELECT c.job_title
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.hire_date DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
