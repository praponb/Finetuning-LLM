SELECT dept_name, CASE WHEN budget >= 3472000 THEN 'high' WHEN budget >= 698000 THEN 'mid' ELSE 'low' END budget_bucket, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) engineering_count
FROM departments
GROUP BY dept_name, budget_bucket;
