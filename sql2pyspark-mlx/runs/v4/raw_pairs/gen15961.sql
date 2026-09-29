SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE location = 'Berlin')
PIVOT (MAX(budget) FOR location IN ('Bangkok', 'Berlin', 'Singapore'));
