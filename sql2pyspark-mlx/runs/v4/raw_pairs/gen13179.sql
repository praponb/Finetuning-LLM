WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE budget > 1267000
),
summary AS (
    SELECT location, COUNT(budget) AS budget_count, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) AS engineering_count
    FROM filtered_departments
    GROUP BY location
)
SELECT location, engineering_count
FROM summary
ORDER BY engineering_count DESC, location
LIMIT 5;
