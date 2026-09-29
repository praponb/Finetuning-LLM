SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE budget < 2672000)
PIVOT (SUM(budget) FOR location IN ('Chiang Mai', 'Tokyo', 'Berlin', 'Bangkok'))
ORDER BY dept_name;
