WITH base AS (
    SELECT *
    FROM departments
    WHERE location NOT IN ('Bangkok', 'Chiang Mai')
),
summary AS (
    SELECT CASE WHEN budget >= 3252000 THEN 'high' WHEN budget >= 1203000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(CASE WHEN location = 'Singapore' THEN 1 END) AS singapore_count, MIN(budget) AS min_budget
    FROM base
    GROUP BY CASE WHEN budget >= 3252000 THEN 'high' WHEN budget >= 1203000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, singapore_count, min_budget
FROM summary
WHERE singapore_count > 7
ORDER BY min_budget DESC, budget_bucket;
