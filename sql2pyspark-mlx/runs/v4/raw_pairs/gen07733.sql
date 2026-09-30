SELECT location, dept_name, MAX(budget) - MIN(budget) AS budget_range, COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count FROM departments GROUP BY location, dept_name;
