SELECT id, location, CASE WHEN budget >= 4390000 THEN 'High' WHEN budget >= 1856000 THEN 'Medium' ELSE 'Low' END budget_level
FROM departments
WHERE location IN ('Berlin', 'Tokyo', 'Singapore') AND dept_name IN ('HR', 'Legal', 'Marketing')
