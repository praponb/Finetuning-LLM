SELECT dept_name, SUM(budget) AS total_budget, SUM(SUM(budget)) OVER (ORDER BY dept_name) AS cumulative_budget FROM departments GROUP BY dept_name
