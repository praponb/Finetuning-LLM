SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE budget BETWEEN 1250000 AND 2719000) PIVOT (COUNT(budget) FOR location IN ('Chiang Mai', 'New York', 'London', 'Bangkok'));
