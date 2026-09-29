SELECT CASE WHEN budget >= 3107000 THEN 'high' WHEN budget >= 643000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(budget) AS budget_count
FROM departments
WHERE location IN ('Bangkok', 'New York', 'London') OR dept_name <> 'Legal'
GROUP BY 1
HAVING COUNT(budget) > 2
ORDER BY budget_count DESC, budget_bucket DESC;
