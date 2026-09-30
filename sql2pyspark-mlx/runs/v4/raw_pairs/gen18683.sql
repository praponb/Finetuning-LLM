SELECT * FROM (SELECT location, dept_name, budget FROM departments) src PIVOT (MAX(budget) AS value_sum FOR dept_name IN ('Engineering', 'Marketing', 'Legal'))
