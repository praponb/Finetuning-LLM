SELECT CASE WHEN budget >= 3777000 THEN 'high' WHEN budget >= 1796000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values
FROM departments
WHERE location IN ('Tokyo', 'Singapore', 'Bangkok') AND dept_name IN ('HR', 'Finance')
GROUP BY 1, 2
ORDER BY budget_bucket;
