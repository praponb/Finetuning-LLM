SELECT location, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
WHERE dept_name IN ('Legal', 'Marketing', 'HR')
GROUP BY location
ORDER BY stddev_budget DESC
