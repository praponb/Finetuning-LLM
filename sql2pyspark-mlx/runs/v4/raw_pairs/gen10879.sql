WITH src AS (
    SELECT *
    FROM employees
    WHERE dept_id IN (2, 4, 6, 18)
),
level_summary AS (
    SELECT level, COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count, COUNT(DISTINCT salary) AS unique_job_titles
    FROM src
    GROUP BY level
)
SELECT level, l4_count
FROM level_summary
WHERE l4_count > 3
