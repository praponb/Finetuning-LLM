WITH src AS (
    SELECT *
    FROM departments
    WHERE dept_name <> 'Legal'
),
departments_stats AS (
    SELECT dept_name, SUM(budget) AS total_budget
    FROM src
    GROUP BY dept_name
)
SELECT *
FROM departments_stats
ORDER BY dept_name
