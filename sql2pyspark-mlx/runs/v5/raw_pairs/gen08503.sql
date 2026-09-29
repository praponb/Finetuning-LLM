SELECT location_group, budget_bucket, avg_budget
FROM (
    SELECT CASE WHEN location IN ('Bangkok', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END AS location_group, CASE WHEN budget >= 4243000 THEN 'high' WHEN budget >= 739000 THEN 'mid' ELSE 'low' END AS budget_bucket, AVG(budget) AS avg_budget
    FROM departments
    GROUP BY CASE WHEN location IN ('Bangkok', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END, CASE WHEN budget >= 4243000 THEN 'high' WHEN budget >= 739000 THEN 'mid' ELSE 'low' END
) AS agg
WHERE avg_budget > 1524000
ORDER BY budget_bucket DESC
