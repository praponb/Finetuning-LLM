SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget <= 3555000) PIVOT (COUNT(budget) FOR dept_name IN ('Finance', 'HR')) ORDER BY location
