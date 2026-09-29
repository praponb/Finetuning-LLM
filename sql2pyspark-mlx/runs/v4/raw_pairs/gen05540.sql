SELECT * FROM (SELECT dept_name, location, budget FROM departments)
PIVOT (COUNT(budget) FOR location IN ('Bangkok', 'Tokyo', 'Singapore'))
ORDER BY dept_name
