WITH filtered_employees AS (
    SELECT *
    FROM employees
    WHERE manager_id IN (8, 20, 22, 26)
),
summary AS (
    SELECT level, MAX(salary) AS max_salary, ROUND((100 * SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_manager
    FROM filtered_employees
    GROUP BY level
)
SELECT level, max_salary, pct_manager
FROM summary
ORDER BY pct_manager DESC, level
LIMIT 3
