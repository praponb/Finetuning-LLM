SELECT * FROM (SELECT dept_name, location, budget FROM departments) PIVOT (AVG(budget) FOR location IN ('New York', 'Singapore', 'Bangkok', 'Tokyo')) ORDER BY dept_name;
