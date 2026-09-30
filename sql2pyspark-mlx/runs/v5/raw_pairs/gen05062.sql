SELECT * FROM (SELECT location, COUNT(DISTINCT budget) AS unique_dept_names, SUM(budget) AS total_budget FROM departments GROUP BY location) AS agg WHERE unique_dept_names < 9
