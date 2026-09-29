SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget > 3932000)
PIVOT (AVG(budget) FOR dept_name IN ('Legal', 'Marketing'))
ORDER BY location;
