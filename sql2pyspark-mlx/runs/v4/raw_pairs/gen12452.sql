SELECT * FROM (SELECT location, dept_name, budget FROM departments) src PIVOT (AVG(budget) AS total FOR dept_name IN ('Legal', 'Engineering'));
