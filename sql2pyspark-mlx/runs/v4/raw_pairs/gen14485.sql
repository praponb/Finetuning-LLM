SELECT CASE WHEN budget >= 3891000 THEN 'high' WHEN budget >= 1104000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range, SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
GROUP BY 1
ORDER BY budget_range DESC;
