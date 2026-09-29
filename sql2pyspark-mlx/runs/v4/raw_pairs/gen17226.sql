SELECT dept_name, MIN(budget) AS min_budget FROM departments GROUP BY dept_name ORDER BY min_budget DESC
