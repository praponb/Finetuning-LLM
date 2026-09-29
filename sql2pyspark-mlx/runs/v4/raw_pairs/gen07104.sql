SELECT * FROM (SELECT dept_name, location, budget FROM departments)
PIVOT (MAX(budget) FOR location IN ('Chiang Mai', 'London', 'Bangkok'))
ORDER BY dept_name
