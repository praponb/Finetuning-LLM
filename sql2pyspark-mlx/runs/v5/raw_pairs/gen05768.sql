SELECT * FROM (SELECT CASE WHEN budget >= 4407000 THEN 'high' WHEN budget >= 1895000 THEN 'mid' ELSE 'low' END AS budget_bucket, dept_name, budget FROM departments)
PIVOT (COUNT(budget) FOR dept_name IN ('Finance', 'Sales', 'Engineering', 'Marketing'))
