SELECT budget_bucket, location_group, max_budget
FROM (
    SELECT CASE WHEN budget >= 4423000 THEN 'high' WHEN budget >= 1687000 THEN 'mid' ELSE 'low' END AS budget_bucket, CASE WHEN location IN ('Chiang Mai', 'New York') THEN 'core' ELSE 'other' END AS location_group, MAX(budget) AS max_budget
    FROM departments
    WHERE location NOT IN ('Chiang Mai', 'New York', 'Berlin')
    GROUP BY CASE WHEN budget >= 4423000 THEN 'high' WHEN budget >= 1687000 THEN 'mid' ELSE 'low' END, CASE WHEN location IN ('Chiang Mai', 'New York') THEN 'core' ELSE 'other' END
) AS agg
WHERE max_budget >= 2955000
ORDER BY 3 DESC, location_group DESC;
