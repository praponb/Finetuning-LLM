SELECT CASE WHEN budget >= 3355000 THEN 'high' WHEN budget >= 1944000 THEN 'mid' ELSE 'low' END AS budget_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values, SUM(budget) AS total_budget
FROM departments
GROUP BY 1
HAVING SUM(budget) > 16584000;
