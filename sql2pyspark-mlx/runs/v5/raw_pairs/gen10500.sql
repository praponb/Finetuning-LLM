SELECT * FROM (SELECT location, dept_name, budget FROM departments)
PIVOT (AVG(budget) FOR dept_name IN ('Legal', 'Finance', 'Sales', 'HR'))
ORDER BY location
