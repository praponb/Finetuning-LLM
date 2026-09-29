SELECT
    budget_bucket,
    location,
    berlin_count,
    max_budget
FROM (
    SELECT
        CASE WHEN budget >= 3425000 THEN 'high' WHEN budget >= 2054000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        location,
        COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count,
        MAX(budget) AS max_budget
    FROM departments
    WHERE location = 'New York'
    GROUP BY CASE WHEN budget >= 3425000 THEN 'high' WHEN budget >= 2054000 THEN 'mid' ELSE 'low' END, location
) AS t
WHERE max_budget >= 2138000
ORDER BY berlin_count DESC, budget_bucket;
