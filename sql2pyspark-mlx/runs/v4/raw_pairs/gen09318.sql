SELECT * FROM (SELECT dept_name, location, budget FROM departments) PIVOT (AVG(budget) FOR location IN ('London', 'Berlin', 'Tokyo')) ORDER BY dept_name
