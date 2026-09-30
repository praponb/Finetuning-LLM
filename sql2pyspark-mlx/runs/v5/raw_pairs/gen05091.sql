SELECT dept_name, budget_range FROM (SELECT dept_name, MAX(budget) - MIN(budget) AS budget_range FROM departments GROUP BY dept_name) AS sub WHERE budget_range < 27;
