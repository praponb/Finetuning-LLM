SELECT CASE WHEN budget >= 3714000 THEN 'high' WHEN budget >= 2056000 THEN 'mid' ELSE 'low' END budget_bucket, location, SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) finance_count
FROM departments
GROUP BY 1, 2
ORDER BY location DESC, finance_count;
