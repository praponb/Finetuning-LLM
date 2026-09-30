WITH recent_departments AS (
    SELECT *, FLOOR(budget) AS budget_whole
    FROM departments
    WHERE (location IN ('Singapore', 'Tokyo') AND budget >= 1154000) OR location NOT IN ('Singapore', 'Berlin')
),
summary AS (
    SELECT CASE WHEN budget >= 3055000 THEN 'high' WHEN budget >= 1513000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(budget_whole) AS budget_whole_count
    FROM recent_departments
    GROUP BY CASE WHEN budget >= 3055000 THEN 'high' WHEN budget >= 1513000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
ORDER BY budget_bucket
LIMIT 50
