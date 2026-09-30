SELECT * FROM (SELECT location, dept_name, budget FROM departments) src PIVOT (SUM(budget) FOR dept_name IN ('Engineering', 'Marketing', 'Finance'));
