WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE location NOT IN ('London', 'Chiang Mai', 'Singapore')
),
departments_stats AS (
    SELECT location, CASE WHEN budget >= 4321000 THEN 'high' WHEN budget >= 1783000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND((100 * SUM(CASE WHEN location = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok
    FROM recent_departments
    GROUP BY location, CASE WHEN budget >= 4321000 THEN 'high' WHEN budget >= 1783000 THEN 'mid' ELSE 'low' END
)
SELECT location, budget_bucket, pct_bangkok
FROM departments_stats
WHERE pct_bangkok > 22
ORDER BY location ASC, budget_bucket ASC
LIMIT 20;
