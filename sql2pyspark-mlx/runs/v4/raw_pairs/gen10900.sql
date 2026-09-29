WITH src AS (
    SELECT *
    FROM employees
    WHERE level = 'L4'
),
agg AS (
    SELECT is_remote, SUM(salary) total_salary
    FROM src
    GROUP BY is_remote
)
SELECT *
FROM agg
WHERE total_salary > 485000
ORDER BY total_salary, is_remote
LIMIT 10;
