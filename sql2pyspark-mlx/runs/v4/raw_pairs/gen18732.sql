SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE dept_name <> 'Engineering') PIVOT (COUNT(budget) FOR location IN ('Berlin', 'New York')) ORDER BY dept_name;
