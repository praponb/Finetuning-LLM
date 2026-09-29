SELECT CASE WHEN budget >= 3794000 THEN 'high' WHEN budget >= 1172000 THEN 'mid' ELSE 'low' END budget_bucket, dept_name, MAX(budget) - MIN(budget) budget_range
FROM departments
WHERE budget BETWEEN 2193000 AND 2448000
GROUP BY CASE WHEN budget >= 3794000 THEN 'high' WHEN budget >= 1172000 THEN 'mid' ELSE 'low' END, dept_name
HAVING SUM(budget) < 12150000
ORDER BY 1
