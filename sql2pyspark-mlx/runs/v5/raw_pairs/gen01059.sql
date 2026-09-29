SELECT dept_name, MIN(budget) AS min_budget, COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count, ROUND(STDDEV(budget), 2) AS stddev_budget FROM departments GROUP BY dept_name;
