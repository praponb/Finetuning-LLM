SELECT budget_bucket, location_list, avg_budget
FROM (
    SELECT CASE WHEN budget >= 3336000 THEN 'high' WHEN budget >= 1500000 THEN 'mid' ELSE 'low' END AS budget_bucket, SORT_ARRAY(COLLECT_SET(location)) AS location_list, ROUND(AVG(budget), 2) AS avg_budget
    FROM departments
    GROUP BY CASE WHEN budget >= 3336000 THEN 'high' WHEN budget >= 1500000 THEN 'mid' ELSE 'low' END
) AS s
WHERE avg_budget > 37
