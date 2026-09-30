SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE dept_name IS NOT NULL)
PIVOT (SUM(budget) FOR location IN ('Bangkok', 'Chiang Mai', 'New York'))
ORDER BY dept_name
