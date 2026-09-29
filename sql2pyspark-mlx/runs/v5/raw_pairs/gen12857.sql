WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'Marketing')
),
departments_stats AS (
    SELECT CASE WHEN budget >= 3614000 THEN 'high' WHEN budget >= 624000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_locations
    FROM recent_departments
    GROUP BY CASE WHEN budget >= 3614000 THEN 'high' WHEN budget >= 624000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, unique_locations
FROM departments_stats;
