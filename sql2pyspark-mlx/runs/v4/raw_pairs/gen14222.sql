SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE location = 'Tokyo')
PIVOT (AVG(budget) FOR dept_name IN ('HR', 'Sales', 'Marketing', 'Finance'))
ORDER BY location
