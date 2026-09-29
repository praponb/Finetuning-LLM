WITH Base AS (
    SELECT *
    FROM employees
    WHERE salary <= 84000
),
summary AS (
    SELECT is_remote, COUNT(DISTINCT salary) AS unique_job_titles
    FROM Base
    GROUP BY is_remote
)
SELECT is_remote, unique_job_titles
FROM summary
WHERE unique_job_titles < 8
