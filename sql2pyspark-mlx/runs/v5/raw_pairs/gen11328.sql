SELECT * FROM (SELECT dept_name, location, budget FROM departments) src PIVOT (AVG(budget) AS amount, COUNT(1) AS n FOR location IN ('New York' AS new_york, 'London' AS london));
