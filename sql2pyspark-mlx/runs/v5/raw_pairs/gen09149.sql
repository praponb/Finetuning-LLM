WITH Base AS (
    SELECT *, COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE location IN ('Singapore', 'Berlin')
),
summary AS (
    SELECT CASE WHEN budget >= 3125000 THEN 'high' WHEN budget >= 1996000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range, COUNT(CASE WHEN location = 'London' THEN 1 END) AS london_count
    FROM Base
    GROUP BY CASE WHEN budget >= 3125000 THEN 'high' WHEN budget >= 1996000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, budget_range, london_count
FROM summary
WHERE london_count > 5
ORDER BY budget_bucket
