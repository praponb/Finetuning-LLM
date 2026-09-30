SELECT
    dept_name,
    CASE WHEN budget >= 4244000 THEN 'high' WHEN budget >= 808000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    MAX(budget) - MIN(budget) AS budget_range,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count
FROM departments
GROUP BY dept_name, CASE WHEN budget >= 4244000 THEN 'high' WHEN budget >= 808000 THEN 'mid' ELSE 'low' END
ORDER BY budget_bucket DESC
