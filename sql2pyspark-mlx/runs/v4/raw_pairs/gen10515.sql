SELECT location, SUM(budget) AS total_budget, COUNT(*) AS cnt, SUM(SUM(budget)) OVER (ORDER BY location) AS cumulative_budget FROM departments WHERE budget > 2708000 GROUP BY location;
